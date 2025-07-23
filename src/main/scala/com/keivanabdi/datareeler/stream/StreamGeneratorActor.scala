package com.keivanabdi.datareeler.stream

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.Actor
import org.apache.pekko.stream.scaladsl.Concat
import org.apache.pekko.stream.scaladsl.Merge
import org.apache.pekko.stream.scaladsl.Source

import scala.reflect.Typeable

import com.keivanabdi.datareeler.models.*
import com.keivanabdi.datareeler.templates.ReelerTemplate
import org.slf4j.LoggerFactory

class StreamGeneratorActor[D: Typeable, MD: Typeable, TI: Typeable](
    broadcastedDataQueuedSource               : Source[D, NotUsed],
    broadcastedMetaDataQueuedSource           : Source[MD, NotUsed],
    broadcastedSystemStateMessagesQueuedSource: Source[TI, NotUsed],
    initialMetaData                           : MD,
    val reelerTemplate                        : ReelerTemplate[D, MD, TI]
) extends Actor {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  import StreamGeneratorActor._

  def createMetaDataStream(
      lastMetaData: MD
  ): Source[ReelElement[D, MD, TI], NotUsed] =
    Source
      .single(lastMetaData)
      .concat(broadcastedMetaDataQueuedSource)
      .map(metadata => ReelElement(None, userMetaData = Some(metadata)))

  def createDataStream(
      previousData: List[D]
  ): Source[ReelElement[D, MD, TI], NotUsed] =
    Source(previousData)
      .concat(broadcastedDataQueuedSource)
      .map(data => ReelElement(Some(data)))

  def createTemplateInstructionsStream(
      previousTemplateInstructions: Option[TI]
  ): Source[ReelElement[D, MD, TI], NotUsed] =
    Source
      .single(previousTemplateInstructions)
      .collect { case Some(x) =>
        x
      }
      .concat(broadcastedSystemStateMessagesQueuedSource)
      .map(systemMessage =>
        ReelElement(None, templateInstructions = Some(systemMessage))
      )

  override def receive: Actor.Receive =
    receive(initialMetaData, None, Nil)

  def receive(
      previousMetaData            : MD,
      previousTemplateInstructions: Option[TI],
      previousData                : List[D]
  ): Actor.Receive = {
    case SetLastMetaData(metaData) =>
      metaData match {
        case metaDataAsM: MD =>
          context.become(
            receive(metaDataAsM, previousTemplateInstructions, previousData)
          )
        case _ =>
          logger.warn(
            s"StreamGeneratorActor received SetLastMetaData with unexpected type: $metaData"
          )
      }
      sender() ! ()

    case SetLastData(data) =>
      data match
        case dataAsD: D =>
          context.become(
            receive(
              previousMetaData             = previousMetaData,
              previousTemplateInstructions = previousTemplateInstructions,
              previousData                 = previousData :+ dataAsD
            )
          )
      sender() ! ()

    case SetLastTemplateInstructions(templateInstructions) =>
      templateInstructions match
        case templateInstructionsAsTI: TI =>
          context.become(
            receive(
              previousMetaData             = previousMetaData,
              previousTemplateInstructions = Some(templateInstructionsAsTI),
              previousData                 = previousData
            )
          )
      sender() ! ()

    case GetStream =>
      val senderActor = sender()
      senderActor !
        Source.combine(
          createDataStream(previousData),
          createTemplateInstructionsStream(previousTemplateInstructions),
          createMetaDataStream(previousMetaData)
        )(Merge(_))

    case SetFinalState =>
      context.become(
        finalStateHandler[D, MD, TI](
          previousData                 = previousData,
          previousTemplateInstructions = previousTemplateInstructions,
          previousMetaData             = previousMetaData
        )
      )
      sender() ! ()

    case unknownMessage =>
      logger.warn(s"Unknown message: $unknownMessage")
  }

  def finalStateHandler[D, MD, TI](
      previousData                : List[D],
      previousTemplateInstructions: Option[TI],
      previousMetaData            : MD
  ): Actor.Receive = { case _ =>
    val senderActor = sender()
    senderActor !
      Source.combine(
        Source(previousData.map(data => ReelElement(userData = Some(data)))),
        Source.single(
          ReelElement(
            userData             = None,
            templateInstructions = previousTemplateInstructions
          )
        ),
        Source.single(
          ReelElement(userData = None, userMetaData = Some(previousMetaData))
        )
      )(Concat(_))
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.warn(
      s"StreamGeneratorActor ${self.path} is restarting due to ${reason.getMessage}"
    )
    super.preRestart(reason, message)
  }

}

/** An Akka/Pekko Actor that manages and generates Akka Streams based on
  * incoming data, metadata, and template instructions. It maintains the last
  * known state of these elements and can provide a combined stream of
  * `ReelElement`s.
  *
  * @param broadcastedDataQueuedSource
  *   A source for new data elements (D).
  * @param broadcastedMetaDataQueuedSource
  *   A source for new metadata elements (MD).
  * @param broadcastedSystemStateMessagesQueuedSource
  *   A source for new template instructions (TI).
  * @param initialMetaData
  *   The initial metadata to start the stream with.
  * @param reelerTemplate
  *   The template used for processing the reel elements.
  * @tparam D
  *   The type of data elements.
  * @tparam MD
  *   The type of metadata elements.
  * @tparam TI
  *   The type of template instructions.
  */
object StreamGeneratorActor {

  /** Message to request the current combined stream of ReelElements.
    */
  case object GetStream

  /** Message to update the last known metadata.
    * @param metaData
    *   The new metadata.
    * @tparam M
    *   The type of the metadata.
    */
  case class SetLastMetaData[M](metaData: M)

  /** Message to update the last known template instructions.
    * @param templateInstructions
    *   The new template instructions.
    * @tparam TI
    *   The type of the template instructions.
    */
  case class SetLastTemplateInstructions[TI](templateInstructions: TI)

  /** Message to add a new data element to the list of previous data.
    * @param data
    *   The new data element.
    * @tparam D
    *   The type of the data element.
    */
  case class SetLastData[D](data: D)

  /** Message to transition the actor to its final state, where it will emit a
    * stream based on the accumulated data and metadata.
    */
  case object SetFinalState

}
