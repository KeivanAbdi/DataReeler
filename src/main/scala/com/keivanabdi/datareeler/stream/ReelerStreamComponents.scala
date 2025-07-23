package com.keivanabdi.datareeler.stream

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.Props
import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.scaladsl.BroadcastHub
import org.apache.pekko.stream.scaladsl.Source

import java.util.UUID
import scala.reflect.Typeable

import com.keivanabdi.datareeler.models.ReelElement
import com.keivanabdi.datareeler.models.ReelerSystemConfig
import com.keivanabdi.datareeler.stream.StreamPacer
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

/** Class responsible for setting up and holding the Akka/Pekko Stream
  * components.
  *
  * @param D
  *   The type of user data.
  * @param MD
  *   The type of user metadata.
  * @param TI
  *   The type of template instructions.
  */
class ReelerStreamComponents[D: Typeable, MD: Typeable, TI: Typeable](
    inputReelElementStream: () => Source[ReelElement[D, MD, TI], ?],
    config                : ReelerSystemConfig[D, MD, TI]
)(using
    val actorSystem: ActorSystem
) {

  import actorSystem.dispatcher
  val logger = LoggerFactory.getLogger(getClass.getName)

  val demandUUID = UUID.randomUUID()
  logger.debug(s"demandUUID uuid: $demandUUID")
  val dataUUID = UUID.randomUUID()
  logger.debug(s"dataUUID uuid: $dataUUID")
  val metaDataUUID = UUID.randomUUID()
  logger.debug(s"metaDataUUID uuid: $metaDataUUID")
  val templateInstructionsUUID = UUID.randomUUID()
  logger.debug(s"templateInstructionsUUID uuid: $templateInstructionsUUID")

  // --- Tracker Actor ---
  lazy val trackerActor: ActorRef =
    actorSystem.actorOf(Props(classOf[TrackerActor]), "tracker-actor")

  // --- Demand Stream Components ---
  lazy val (demandQueue, demandQueuedSource) =
    Source
      .queue[DateTime](
        bufferSize       = config.demandBufferSize,
        overflowStrategy = OverflowStrategy.backpressure
      )
      .preMaterialize()

  lazy val (
    trackedDemandQueue,
    trackingDemandFlow,
    demandEmptinessSignalStreamMaker
  ) =
    TrackedSourceQueue.createWithFlow[DateTime, ReelElement[D, MD, TI]](
      queue             = demandQueue,
      trackerActor      = trackerActor,
      ref               = demandUUID,
      trackingCondition = _.userData.isDefined
    )

  // --- Data Stream Components ---
  lazy val (dataQueue, dataQueuedSource) =
    Source
      .queue[D](
        bufferSize       = config.dataBufferSize,
        overflowStrategy = OverflowStrategy.backpressure
      )
      .preMaterialize()

  lazy val (
    trackedDataQueue,
    trackingDataFlow,
    dataEmptinessSignalStreamMaker
  ) =
    TrackedSourceQueue.createWithFlow[D, D](
      queue        = dataQueue,
      trackerActor = trackerActor,
      ref          = dataUUID
    )

  lazy val broadcastedDataQueuedSource: Source[D, NotUsed] =
    dataQueuedSource.via(trackingDataFlow).runWith(BroadcastHub.sink(256))

  // --- MetaData Stream Components ---
  lazy val (metaDataQueue, metaDataQueuedSource) =
    Source
      .queue[MD](
        bufferSize       = config.metaDataBufferSize,
        overflowStrategy = OverflowStrategy.backpressure
      )
      .preMaterialize()

  lazy val (
    trackedMetaDataQueue,
    trackingMetaDataFlow,
    metadataEmptinessSignalStreamMaker
  ) =
    TrackedSourceQueue.createWithFlow[MD, MD](
      queue        = metaDataQueue,
      trackerActor = trackerActor,
      ref          = metaDataUUID
    )

  lazy val broadcastedMetaDataQueuedSource: Source[MD, NotUsed] =
    metaDataQueuedSource
      .via(trackingMetaDataFlow)
      .runWith(BroadcastHub.sink(256))

  // --- Template Instructions Messages Stream Components ---
  lazy val (
    templateInstructionsMessagesQueue,
    templateInstructionsQueuedSource
  ) =
    Source
      .queue[TI](
        bufferSize       = config.templateInstructionsBufferSize,
        overflowStrategy = OverflowStrategy.backpressure
      )
      .preMaterialize()

  lazy val (
    trackedSystemStateMessagesQueue,
    trackingTemplateInstructionsFlow,
    systemMessagesEmptinessSignalStreamMaker
  ) =
    TrackedSourceQueue.createWithFlow[TI, TI](
      queue        = templateInstructionsMessagesQueue,
      trackerActor = trackerActor,
      ref          = templateInstructionsUUID
    )

  lazy val broadcastedTemplateInstructionsQueuedSource: Source[TI, NotUsed] =
    templateInstructionsQueuedSource
      .via(
        trackingTemplateInstructionsFlow
      )
      .runWith(
        BroadcastHub.sink(config.templateInstructionsBufferSize)
      )

  // --- Stream Generator Actor ---
  lazy val streamActor: ActorRef = actorSystem.actorOf(
    Props(
      new StreamGeneratorActor[D, MD, TI](
        broadcastedDataQueuedSource     = broadcastedDataQueuedSource,
        broadcastedMetaDataQueuedSource = broadcastedMetaDataQueuedSource,
        broadcastedSystemStateMessagesQueuedSource =
          broadcastedTemplateInstructionsQueuedSource,
        initialMetaData = config.initialMetaData(),
        reelerTemplate  = config.reelerTemplate
      )
    ),
    "stream-actor"
  )

  // --- Paced Input and Demand Sources ---
  lazy val (
    slowPacedInputReelElementStream: Source[ReelElement[D, MD, TI], NotUsed],
    slowPacedDemandQueuedSource    : Source[DateTime, NotUsed]
  ) =
    StreamPacer.evenPacer(
      fastSource = Source.lazySource(inputReelElementStream),
      slowSource = demandQueuedSource,
      slowSourceEmptinessSignal = demandEmptinessSignalStreamMaker()
        .map {
          case (_, 0) => false
          case _      => true
        }
    )

}
