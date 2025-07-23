package com.keivanabdi.datareeler.stream

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.pattern.ask
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.*

import com.keivanabdi.datareeler.models.ReelElement
import org.slf4j.LoggerFactory

/** Processes the slow-paced input stream of ReelElements.
  *
  * @param D
  *   The type of user data.
  * @param MD
  *   The type of user metadata.
  * @param TI
  *   The type of template instructions.
  */
class InputReelProcessor[D, MD, TI](
    components: ReelerStreamComponents[D, MD, TI]
)(using
    ActorSystem,
    ExecutionContext,
    Timeout
) {

  val logger = LoggerFactory.getLogger(getClass.getName)

  /** Processes the slow-paced input stream of ReelElements.
    *
    * Handles offering data/metadata to tracked queues, signaling the stream
    * actor, waiting for queues to empty, setting the final state on the actor,
    * and completing queues.
    *
    * @param slowPacedInputReelElementStream
    *   The input stream to process.
    * @return
    *   A Future indicating completion or failure of the stream processing.
    */
  def run(
      slowPacedInputReelElementStream: Source[ReelElement[D, MD, TI], NotUsed]
  ): Future[Unit] = {

    // Helper function to process a single ReelElement (uses components instance)
    def processReelElement(
        element: ReelElement[D, MD, TI]
    ): Future[ReelElement[D, MD, TI]] = {
      val metaDataFuture = element.userMetaData
        .map { metaData =>
          (components.streamActor ? StreamGeneratorActor.SetLastMetaData(
            metaData
          ))
            .flatMap(_ => components.trackedMetaDataQueue.offer(metaData))
        }
        .getOrElse(Future.successful(()))

      val dataFuture = element.userData
        .map { data =>
          (components.streamActor ? StreamGeneratorActor.SetLastData(data))
            .flatMap(_ => components.trackedDataQueue.offer(data))
        }
        .getOrElse(Future.successful(()))

      for {
        _ <- metaDataFuture
        _ <- dataFuture
      } yield element // Return the original element to keep the stream flowing
    }

    // Helper function to wait for queues to empty and set final state (uses components instance)
    def waitForQueuesAndSetFinalState(): Future[Unit] = {
      val dataEmptyFuture =
        components
          .dataEmptinessSignalStreamMaker()
          .filter(_._2 == 0)
          .runWith(Sink.head)
      val metaDataEmptyFuture =
        components
          .metadataEmptinessSignalStreamMaker()
          .filter(_._2 == 0)
          .runWith(Sink.head)

      val streamDrainageWaitPromise = Promise[Unit]()

      components.actorSystem.scheduler
        .scheduleOnce( // TODO: A fixed delay is used to ensure the stream drains. This should be replaced with a more robust mechanism.
          10.seconds,
          () => {
            streamDrainageWaitPromise.success(())
            ()
          }
        )

      for {
        _ <- dataEmptyFuture
        _ <- metaDataEmptyFuture
        _ <- components.streamActor ? StreamGeneratorActor.SetFinalState
        _ <- streamDrainageWaitPromise.future
      } yield ()
    }

    // Helper function for final queue completion and logging (uses components instance)
    def completeQueues(): Unit = {
      logger.info(
        "Completing tracked data/metadata queues and demand/data queues."
      )

      components.demandQueue.complete()
      components.trackedMetaDataQueue.complete()
      components.trackedDataQueue.complete()
      components.dataQueue.complete()
    }

    // --- Stream Execution ---
    slowPacedInputReelElementStream
      .via(components.trackingDemandFlow)
      .mapAsync(1)(processReelElement(_))
      .run() // Execute the stream processing part
      .flatMap { _ =>
        logger.info(
          "Input stream processing finished, waiting for queues to empty..."
        )
        waitForQueuesAndSetFinalState()
      }
      .map { _ => // Perform cleanup after final state is set
        logger.info("Queues emptied and final state set, completing queues...")
        completeQueues()
      }
      .recoverWith { // Centralized error handling for the entire process (uses components instance)
        case e: Throwable =>
          logger.error(
            s"Error during slow-paced input stream consumption or cleanup: ${e.getMessage}",
            e
          )
          components.trackedDataQueue.fail(e)
          components.trackedMetaDataQueue.fail(e)
          components.demandQueue.fail(e)
          components.dataQueue.fail(e)
          Future.failed(e)
      }
  }

}
