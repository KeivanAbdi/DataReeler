package com.keivanabdi.datareeler.stream

import org.apache.pekko.Done
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.ask
import org.apache.pekko.stream.QueueOfferResult
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.util.Timeout

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import org.joda.time.DateTime
import org.slf4j.LoggerFactory

/** Companion object for [[TrackedSourceQueue]] providing a factory method to
  * create a new instance along with a tracking flow and a stream to monitor
  * queue emptiness.
  */
object TrackedSourceQueue {

  val logger = LoggerFactory.getLogger(getClass.getName)

  /** Creates a new [[TrackedSourceQueue]] instance, a corresponding Akka Stream
    * `Flow` for tracking, and a function to get a `Source` that emits queue
    * size changes.
    *
    * This method sets up the necessary components to track elements as they are
    * offered to the queue and as they pass through a subsequent flow.
    *
    * @param queue
    *   The underlying `SourceQueueWithComplete` to be wrapped and tracked.
    * @param trackerActor
    *   The `ActorRef` of the [[TrackerActor]] responsible for managing queue
    *   size state.
    * @param trackingCondition
    *   A predicate function that determines if an element passing through the
    *   `trackingFlow` should trigger a decrease in the tracked queue size.
    *   Defaults to `true` for all elements.
    * @param ref
    *   A unique identifier for this specific queue, used by the `trackerActor`
    *   to manage its state.
    * @param ec
    *   The implicit `ExecutionContext` for asynchronous operations.
    * @tparam I
    *   The type of elements offered to the `SourceQueueWithComplete`.
    * @tparam J
    *   The type of elements processed by the `trackingFlow`.
    * @return
    *   A tuple containing:
    *   - The [[TrackedSourceQueue]] instance.
    *   - A `Flow[J, J, NotUsed]` that should be integrated into the Akka Stream
    *     downstream of the queue. Elements passing through this flow (and
    *     satisfying `trackingCondition`) will decrement the tracked queue size.
    *   - A function `() => Source[(DateTime, Int), Future[NotUsed]]` that, when
    *     called, returns a `Source` emitting tuples of `(DateTime,
    *     currentQueueSize)` whenever the queue size changes.
    */
  def createWithFlow[I, J](
      queue            : SourceQueueWithComplete[I],
      trackerActor     : ActorRef,
      trackingCondition: J => Boolean = (_: J) => true,
      ref              : UUID
  )(using
      ec: ExecutionContext
  ): (
      TrackedSourceQueue[I],
      Flow[J, J, NotUsed],
      () => Source[(DateTime, Int), Future[NotUsed]]
  ) = {

    given Timeout = 30.seconds

    trackerActor ! TrackerActor.RegisterRef(ref)

    val getEmptinessStream: () => Source[(DateTime, Int), Future[NotUsed]] =
      () =>
        Source.futureSource(
          (trackerActor ? TrackerActor.GetQueueSizeChangesStream(ref))
            .mapTo[Source[(DateTime, Int), NotUsed]]
        )

    val trackingFlow: Flow[J, J, NotUsed] =
      Flow[J]
        .wireTap { j =>
          // Side effect happens here, just before element passes through.
          if (trackingCondition(j)) {
            trackerActor ! TrackerActor.DecreaseQueueSize(ref)
          }
        }

    (
      new TrackedSourceQueue[I](queue, trackerActor, ref),
      trackingFlow,
      getEmptinessStream
    )
  }

}

/** A wrapper around `SourceQueueWithComplete` that integrates with a
  * [[TrackerActor]] to keep track of the number of elements currently in the
  * queue.
  *
  * When an element is offered to this queue, it sends an `IncreaseQueueSize`
  * message to the `trackerActor`. The corresponding `DecreaseQueueSize` message
  * is expected to be sent by a downstream flow (e.g., using the `trackingFlow`
  * created by `TrackedSourceQueue.createWithFlow`) once an element has been
  * processed.
  *
  * @param queue
  *   The underlying `SourceQueueWithComplete` to which elements are actually
  *   offered.
  * @param trackerActor
  *   The `ActorRef` of the [[TrackerActor]] responsible for managing queue size
  *   state.
  * @param ref
  *   A unique identifier for this specific queue, used by the `trackerActor`.
  * @param ec
  *   The implicit `ExecutionContext` for asynchronous operations.
  * @tparam T
  *   The type of elements that can be offered to this queue.
  */
class TrackedSourceQueue[T] private (
    private val queue       : SourceQueueWithComplete[T],
    private val trackerActor: ActorRef,
    ref                     : UUID
)(using ec: ExecutionContext)
    extends SourceQueueWithComplete[T] {

  val logger = LoggerFactory.getLogger(getClass.getName)

  /** Fails the underlying queue with the given exception.
    * @param ex
    *   The exception to fail the queue with.
    */
  override def fail(ex: Throwable): Unit = queue.fail(ex)

  /** Offers an element to the underlying queue. If the element is successfully
    * enqueued, an `IncreaseQueueSize` message is sent to the `trackerActor`.
    * @param elem
    *   The element to offer.
    * @return
    *   A `Future` that will be completed with a `QueueOfferResult`.
    */
  override def offer(elem: T): Future[QueueOfferResult] = {
    for {
      result <- queue.offer(elem)
    } yield {
      result match {
        case QueueOfferResult.Enqueued =>
          trackerActor ! TrackerActor.IncreaseQueueSize(ref)

        case QueueOfferResult.Dropped =>
          trackerActor ! TrackerActor.ElementDropped(ref)
        case _ =>
          // Log other results for debugging purposes
          logger.warn(
            s"Offer to queue resulted in $result for ref $ref. Element was not enqueued."
          )
      }
      result
    }

  }

  /** Completes the underlying queue.
    */
  override def complete(): Unit = queue.complete()

  /** Returns a `Future` that will be completed when the underlying queue
    * completes.
    * @return
    *   A `Future[Done]` indicating completion.
    */
  override def watchCompletion(): Future[Done] = queue.watchCompletion()

}
