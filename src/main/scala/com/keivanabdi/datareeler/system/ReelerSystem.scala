package com.keivanabdi.datareeler.system

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent
import org.apache.pekko.http.scaladsl.server.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.pattern.ask
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.*
import scala.reflect.Typeable

import com.keivanabdi.datareeler.models.ReelElement
import com.keivanabdi.datareeler.models.ReelerSystemConfig
import com.keivanabdi.datareeler.stream.DemandProcessor
import com.keivanabdi.datareeler.stream.InputReelProcessor
import com.keivanabdi.datareeler.stream.ReelerStreamComponents
import com.keivanabdi.datareeler.stream.StreamGeneratorActor
import com.keivanabdi.datareeler.system.ReelHttpSystem
import io.circe.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

/** Orchestrates the entire data reeling system, integrating stream components,
  * processors, and providing an HTTP interface for interaction (e.g., SSE
  * stream, load-more signal).
  *
  * @param D
  *   The type of user data.
  * @param MD
  *   The type of user metadata.
  * @param TI
  *   The type of template instructions.
  */
class ReelerSystem[D: Typeable, MD: Typeable, TI: Typeable](
    inputReelElementStream: () => Source[ReelElement[D, MD, TI], ?],
    config                : ReelerSystemConfig[D, MD, TI],
    userRoutes            : Seq[Route]
)(using
    actorSystem: ActorSystem
) {

  val logger = LoggerFactory.getLogger(getClass.getName)

  // Make ExecutionContext and Timeout available implicitly from ActorSystem and constructor
  import actorSystem.dispatcher
  given Timeout = config.timeout

  // Instantiate the stream components using the new class
  private val components: ReelerStreamComponents[D, MD, TI] =
    new ReelerStreamComponents(inputReelElementStream, config)

  // Instantiate the processors, passing necessary implicits
  private val inputProcessor  = new InputReelProcessor(components)
  private val demandProcessor = new DemandProcessor(components, config)

  // Promise/Future for signaling
  private val initializationPromise: Promise[Unit] = Promise[Unit]()

  // --- Exception Handler ---
  val myExceptionHandler: ExceptionHandler = ExceptionHandler {
    case e: Exception =>
      extractUri { uri =>
        logger.error(
          s"Unhandled exception during request processing for $uri: ${e.getMessage}",
          e
        )
        complete(
          org.apache.pekko.http.scaladsl.model.StatusCodes.InternalServerError,
          "Internal server error"
        )
      }
  }

  // --- Stream Logic ---

  // Future source for the main event stream (uses components)
  def finalStreamFuture(): Future[Source[ReelElement[D, MD, TI], NotUsed]] =
    (components.streamActor ? StreamGeneratorActor.GetStream)
      .mapTo[Source[ReelElement[D, MD, TI], Any]]
      .map(_.mapMaterializedValue(_ => NotUsed))

  // Source emitting a single "waiting" element initially
  def createWaitingSource(): Source[ReelElement[D, MD, TI], NotUsed] =
    Source.single(ReelElement(None, None, None))

  // Future that completes when initialization is done and starts the background consumers (uses processor instances)
  lazy val startConsumersFuture: Future[Unit] =
    initializationPromise.future.map { _ =>
      // Call run methods on the processor instances
      demandProcessor.run(
        slowPacedDemandQueuedSource = components.slowPacedDemandQueuedSource
      )
      inputProcessor.run(
        slowPacedInputReelElementStream =
          components.slowPacedInputReelElementStream
      )
    }

  def loadMoreSignal(): Future[Unit] = {
    initializationPromise.trySuccess(())
    logger.info("'/load-more' triggered, initializing stream and consumers.")
    val now = DateTime.now()
    logger.debug(
      s"Offering ${config.demandBatchSize} demand items to trackedDemandQueue."
    )
    Future
      .traverse(0 until config.demandBatchSize) { _ =>
        components.trackedDemandQueue.offer(now)
      }
      .map(_ => ())
  }

  def serverSentStream(): Source[ServerSentEvent, NotUsed] =
    createWaitingSource()
      .concat(
        // Once initialization is triggered, switch to the actual stream
        Source.futureSource[ReelElement[D, MD, TI], NotUsed] {
          logger.debug("SSE stream waiting for initialization...")
          startConsumersFuture.flatMap {
            _ => // Waits for the promise completion
              logger.debug(
                "Initialization complete, fetching final stream for SSE..."
              )
              finalStreamFuture()
          }
        }
      )
      .concat(
        // Append a final "stream finished" element
        Source.single(
          ReelElement[D, MD, TI](
            userData     = None,
            userMetaData = None,
            templateInstructions =
              Some(config.reelerTemplate.streamFinishedInstruction)
          )
        )
      )
      .statefulMapConcat { () =>
        var lastUserMetaData        : Option[MD] = None
        var lastTemplateInstructions: Option[TI] = None

        { element =>
          val result = Iterable.single(
            (element, lastUserMetaData, lastTemplateInstructions)
          )
          if (element.userMetaData.isDefined) {
            lastUserMetaData = element.userMetaData
          }
          if (element.templateInstructions.isDefined) {
            lastTemplateInstructions = element.templateInstructions
          }
          result
        }
      }
      .collect {
        case (
              reelElement             : ReelElement[D, MD, TI],
              lastUserMetaData        : Option[MD],
              lastTemplateInstructions: Option[TI]
            ) if !reelElement.isEmpty =>
          ServerSentEvent(
            config.reelerTemplate
              .renderReelElement(
                element                  = reelElement,
                lastUserMetaData         = lastUserMetaData,
                lastTemplateInstructions = lastTemplateInstructions
              )
              .asJson
              .printWith(Printer.spaces2.copy(dropNullValues = true))
          )
      }
      .keepAlive(
        1.second,
        () => ServerSentEvent.heartbeat
      )
      .recoverWith { case e: Throwable =>
        logger.error(s"Error in SSE stream processing: ${e.getMessage}", e)
        // Decide how to handle stream errors, e.g., emit an error event or terminate.
        Source.empty
      }

  val reelHttpSystem: ReelHttpSystem[D, MD, TI] =
    new ReelHttpSystem(
      config     = config,
      userRoutes = userRoutes
    )

  def start(): Future[ServerBinding] =
    reelHttpSystem.start(
      loadMoreSignal   = loadMoreSignal,
      serverSentStream = serverSentStream
    )

}
