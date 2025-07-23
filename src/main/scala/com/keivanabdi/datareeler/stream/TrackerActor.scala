package com.keivanabdi.datareeler.stream

import org.apache.pekko.actor.Actor
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.scaladsl.Source

import java.util.UUID

import com.keivanabdi.datareeler.stream.TrackerActor._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

/** An Akka/Pekko Actor that tracks the size of multiple queues identified by
  * UUIDs.
  *
  * It maintains a map of `UUID` to `QueueInfo`, where `QueueInfo` contains the
  * current size of a queue and a list of `ActorRef`s that are interested in
  * receiving updates about that queue's size changes.
  *
  * This actor handles messages to:
  *   - `IncreaseQueueSize`: Increments the size of a specified queue.
  *   - `DecreaseQueueSize`: Decrements the size of a specified queue.
  *   - `RegisterRef`: Registers a new queue for tracking, initializing its size
  *     to 0.
  *   - `GetQueueSizeChangesStream`: Provides an Akka Stream `Source` that emits
  *     `(DateTime, Int)` tuples whenever the corresponding queue's size
  *     changes.
  */
class TrackerActor extends Actor {
  val logger = LoggerFactory.getLogger(getClass.getName)

  import context.system

  /** The initial receive behavior of the actor, starting with an empty context
    * map.
    */
  def receive: Actor.Receive =
    receive(Map.empty)

  /** Notifies all registered subscribers for a given queue about its current
    * size.
    *
    * @param uuid
    *   The unique identifier of the queue whose subscribers should be notified.
    * @param contextMap
    *   The current state of the actor, containing all tracked queue
    *   information.
    * @return
    *   `Right(())` if notifications were sent, `Left(())` if the UUID was not
    *   found.
    */
  def notifyChanges(
      uuid      : UUID,
      contextMap: Map[UUID, QueueInfo]
  ): Either[Unit, Unit] = {
    val now = DateTime.now()

    contextMap.get(uuid) match
      case Some(value) =>
        value.actorRefs.foreach(_ ! (now -> value.queueSize))
        Right(())
      case None =>
        Left(())
  }

  /** The main receive behavior of the actor, handling various messages to
    * update and query queue states.
    *
    * @param contextMap
    *   The immutable map holding the current state of all tracked queues.
    * @return
    *   An `Actor.Receive` partial function.
    */
  def receive(contextMap: Map[UUID, QueueInfo]): Actor.Receive = {
    case IncreaseQueueSize(uuid) =>
      contextMap.get(uuid) match {
        case Some(previousQueueInfo) =>
          val nextQueueInfo: QueueInfo =
            previousQueueInfo.copy(queueSize = previousQueueInfo.queueSize + 1)
          logger.debug(s"Queue $uuid increased to ${nextQueueInfo.queueSize}")

          val nextContextMap: Map[UUID, QueueInfo] =
            contextMap + (uuid -> nextQueueInfo)

          context.become(
            receive(
              contextMap = nextContextMap
            )
          )
          notifyChanges(uuid, nextContextMap)
        case None =>
          logger.error(
            s"IncreaseQueueSize received for unregistered UUID: $uuid"
          )
      }

    case DecreaseQueueSize(uuid) =>
      contextMap.get(uuid) match {
        case Some(previousQueueInfo) =>
          val nextQueueInfo: QueueInfo =
            previousQueueInfo.copy(queueSize = previousQueueInfo.queueSize - 1)
          logger.debug(s"Queue $uuid decreased to ${nextQueueInfo.queueSize}")

          val nextContextMap: Map[UUID, QueueInfo] =
            contextMap + (uuid -> nextQueueInfo)

          context.become(
            receive(
              contextMap = nextContextMap
            )
          )
          notifyChanges(uuid, nextContextMap)
        case None =>
          logger.error(
            s"DecreaseQueueSize received for unregistered UUID: $uuid"
          )
      }

    case RegisterRef(uuid) =>
      logger.info(s"Registering queue with UUID: $uuid")

      val nextQueueInfo: QueueInfo =
        QueueInfo(Nil, 0)

      val nextContextMap: Map[UUID, QueueInfo] =
        contextMap + (uuid -> nextQueueInfo)

      context.become(
        receive(
          contextMap = nextContextMap
        )
      )

    case GetQueueSizeChangesStream(uuid) =>
      logger.info(s"Preparing a size change stream for queue: $uuid")

      val (actorRef, source) =
        Source
          .actorRef[(DateTime, Int)](10, OverflowStrategy.dropHead)
          .preMaterialize()

      contextMap.get(uuid) match {
        case Some(value) =>
          actorRef ! (DateTime.now() -> value.queueSize)
          val nextQueueInfo: QueueInfo = value.withNewSubscriber(actorRef)
          val nextContextMap: Map[UUID, QueueInfo] =
            contextMap + (uuid -> nextQueueInfo)

          context.become(
            receive(
              contextMap = nextContextMap
            )
          )
          sender() ! source
        case None =>
          logger.error(
            s"GetQueueSizeChangesStream received for unregistered UUID: $uuid. Returning empty source."
          )
          sender() ! Source.empty
      }

    case _: Any =>
      logger.warn("Received unknown message in TrackerActor.")
  }

  /** Called when the actor is stopped. Logs the stop event. */
  override def postStop(): Unit = {
    logger.info("TrackerActor stopped.")
    super.postStop()
  }

  /** Called before the actor is restarted. Logs the restart reason.
    * @param reason
    *   The `Throwable` that caused the restart.
    * @param message
    *   The message that was being processed when the failure occurred, if any.
    */
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error(
      s"TrackerActor ${self.path} is about to restart due to ${reason.getMessage}",
      reason
    )
    super.preRestart(reason, message)
  }

}

/** Companion object for [[TrackerActor]] containing message definitions and
  * utility case classes.
  */
object TrackerActor {

  sealed trait Command

  /** Message to increase the size of a tracked queue.
    * @param ref
    *   The UUID of the queue to increase.
    */
  final case class IncreaseQueueSize(ref: UUID) extends Command

  /** Represents an event where a tracked element has been dropped.
    * @param ref
    *   The UUID of the element that was dropped.
    */
  final case class ElementDropped(ref: UUID) extends Command

  /** Message to decrease the size of a tracked queue.
    * @param ref
    *   The UUID of the queue to decrease.
    */
  final case class DecreaseQueueSize(ref: UUID) extends Command

  /** Message to register a new queue for tracking.
    * @param ref
    *   The UUID of the queue to register.
    */
  final case class RegisterRef(ref: UUID) extends Command

  /** Message to request a stream of queue size changes for a specific queue.
    * @param ref
    *   The UUID of the queue for which to get the stream.
    */
  final case class GetQueueSizeChangesStream(ref: UUID) extends Command

  /** Case class representing the information about a tracked queue.
    * @param actorRefs
    *   A sequence of `ActorRef`s that are subscribed to receive size change
    *   updates for this queue.
    * @param queueSize
    *   The current size of the queue.
    */
  final case class QueueInfo(actorRefs: Seq[ActorRef], queueSize: Int)
      extends Command {

    /** Checks if the queue is empty. */
    def isEmpty(): Boolean = queueSize == 0

    /** Returns a new `QueueInfo` instance with an additional subscriber.
      * @param actorRef
      *   The `ActorRef` of the new subscriber.
      * @return
      *   A new `QueueInfo` instance.
      */
    def withNewSubscriber(actorRef: ActorRef): QueueInfo = {
      QueueInfo(actorRefs :+ actorRef, queueSize)
    }

  }

}
