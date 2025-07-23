package com.keivanabdi.datareeler.stream

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem // Import ActorSystem
import org.apache.pekko.pattern.ask
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import com.keivanabdi.datareeler.models.ReelerSystemConfig
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

/** Processes the slow-paced demand queue source.
  *
  * @param D
  *   The type of user data.
  * @param MD
  *   The type of user metadata.
  * @param TI
  *   The type of template instructions.
  */
class DemandProcessor[D, MD, TI](
    components: ReelerStreamComponents[D, MD, TI],
    config    : ReelerSystemConfig[D, MD, TI]
)(using
    actorSystem: ActorSystem,
    ec         : ExecutionContext,
    timeout    : Timeout
) {

  val logger = LoggerFactory.getLogger(getClass.getName)

  /** Processes the slow-paced demand queue source.
    *
    * Transforms demand signals into template instructions based on queue
    * emptiness, filters duplicates, signals the stream actor, offers
    * instructions to the tracked queue, and handles cleanup.
    *
    * @param slowPacedDemandQueuedSource
    *   The demand source stream to process.
    * @return
    *   A Future indicating completion or failure of the stream processing.
    */
  def run(
      slowPacedDemandQueuedSource: Source[DateTime, NotUsed]
  ): Future[Unit] = {

    // --- Inner Helper Functions ---

    /** Creates a Flow that zips the incoming demand timestamps with the latest
      * demand queue emptiness signal and transforms them into appropriate
      * template instructions (TI). Captures
      * `components.demandEmptinessSignalStreamMaker` and
      * `config.reelerTemplate`.
      */
    def demandSignalToTemplateInstructionsFlow: Flow[DateTime, TI, NotUsed] = {
      Flow[DateTime]
        .map(Some(_))
        .prepend(Source.single(Option.empty[DateTime]))
        .zipLatest(
          components
            .demandEmptinessSignalStreamMaker()
            .collect {
              case (time, 0) => Right(time) // Empty signal with timestamp
              case (_, size) => Left(size)  // NonEmpty signal with size
            }
        )
        .collect {
          case (None, Left(size)) =>
            config.reelerTemplate
              .previouslyRequestedItemsNotProcessedInstruction(size)
          // If demand queue is empty *after* the demand signal time, send 'processed' instruction
          case (Some(demandTime), Right(emptySignalTime))
              if emptySignalTime.isAfter(demandTime) =>
            logger.debug("Demand queue empty, signaling processed.")
            config.reelerTemplate.previouslyRequestedItemsProcessedInstruction
          // If demand queue is not empty, send 'not processed' instruction with size
          case (_, Left(size)) =>
            logger.debug(
              s"Demand queue has $size items, signaling not processed."
            )
            config.reelerTemplate
              .previouslyRequestedItemsNotProcessedInstruction(size)
        }
    }

    /** Creates a Flow that filters out consecutive duplicate elements in a
      * stream.
      */
    def filterConsecutiveDuplicatesFlow[T]: Flow[T, T, NotUsed] = {
      Flow[T]
        .scan[(Option[T], Boolean)]((Option.empty[T], true)) {
          case ((prevOpt, _), current) =>
            val shouldKeep = prevOpt.isEmpty ||
              prevOpt.get != current // Emit if first or different from previous
            (Some(current), shouldKeep)
        }
        .collect { case (Some(item), true) =>
          item
        } // Only emit if shouldKeep is true
    }

    /** Helper function that processes each incoming template instructions
      * Explicitly takes ExecutionContext and Timeout.
      */
    def processTemplateInstructions(templateInstructions: TI)(using
        ec     : ExecutionContext,
        timeout: Timeout
    ): Future[Unit] = {
      (
        components.streamActor ?
          StreamGeneratorActor.SetLastTemplateInstructions(
            templateInstructions
          )
      )
        .flatMap(_ =>
          components.trackedSystemStateMessagesQueue.offer(templateInstructions)
        )
        .map(_ => ())
    }

    /** Waits for the template instructions queue to become empty and then
      * completes the queue. Captures
      * `components.systemMessagesEmptinessSignalStreamMaker` and
      * `components.trackedSystemStateMessagesQueue`. Explicitly takes
      * ActorSystem (for Materializer) and ExecutionContext.
      */
    def cleanupTemplateInstructionsQueue(): Future[Unit] = {
      logger.debug("Waiting for template instructions queue to become empty...")
      components
        .systemMessagesEmptinessSignalStreamMaker()
        .filter(_._2 == 0)
        .runWith(Sink.head)
        .map { _ =>
          logger.info(
            "System messages queue is empty. Completing tracked system state messages queue."
          )
          components.trackedSystemStateMessagesQueue.complete()
        }
    }

    // --- Stream Execution ---
    // Pass implicits from outer scope when calling helper functions
    slowPacedDemandQueuedSource
      .via(demandSignalToTemplateInstructionsFlow)
      .via(filterConsecutiveDuplicatesFlow[TI])
      .mapAsync(1)(processTemplateInstructions(_))
      .run()
      .flatMap { _ =>
        logger.info("Demand source processing completed successfully.")
        cleanupTemplateInstructionsQueue()
      }
      .recoverWith { // Handle errors from either processing or cleanup
        case e: Throwable =>
          logger.error(
            s"Error in slowPacedDemandQueuedSourceConsumption pipeline: ${e.getMessage}",
            e
          )
          components.trackedSystemStateMessagesQueue.fail(e)
          Future.failed(e)
      }
  }

}
