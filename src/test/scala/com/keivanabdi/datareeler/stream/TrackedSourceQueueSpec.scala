package com.keivanabdi.datareeler.stream

import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.stream.{OverflowStrategy, QueueOfferResult}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.scaladsl.SourceQueueWithComplete
import org.apache.pekko.stream.testkit.scaladsl.TestSink
import org.apache.pekko.testkit.{ImplicitSender, TestKit, TestProbe}

import java.util.UUID
import scala.concurrent.duration._

import com.keivanabdi.datareeler.stream.TrackedSourceQueue
import com.keivanabdi.datareeler.stream.TrackerActor
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.concurrent.ScalaFutures._
import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class TrackedSourceQueueSpec
    extends TestKit(ActorSystem("TrackedSourceQueueSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with ImplicitSender {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 3.seconds, interval = 50.millis)

  private given ec: scala.concurrent.ExecutionContext = system.dispatcher

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "TrackedSourceQueue" should {
    "send IncreaseQueueSize to TrackerActor on successful offer" in {
      val trackerProbe = new TestProbe(system)
      val ref          = UUID.randomUUID()
      val sourceWithQueue: Source[String, SourceQueueWithComplete[String]] =
        Source.queue[String](10, OverflowStrategy.dropHead)
      val queue = sourceWithQueue.toMat(Sink.ignore)(Keep.left).run()

      val (trackedQueue, _, _) =
        TrackedSourceQueue.createWithFlow[String, String](
          queue,
          trackerProbe.ref,
          ref = ref
        )

      trackerProbe.expectMsg(TrackerActor.RegisterRef(ref))

      whenReady(trackedQueue.offer("test-element")) { result =>
        result shouldBe QueueOfferResult.Enqueued
      }

      trackerProbe.expectMsg(TrackerActor.IncreaseQueueSize(ref))
    }

    "not send IncreaseQueueSize to TrackerActor on failed offer (e.g., Dropped)" in {
      val trackerProbe = new TestProbe(system)
      val ref          = UUID.randomUUID()
      val sourceWithQueue: Source[String, SourceQueueWithComplete[String]] =
        Source.queue[String](1, OverflowStrategy.dropNew)

      val (queue, downstreamProbe) = sourceWithQueue
        .toMat(TestSink.probe[String])(Keep.both)
        .run()

      val (trackedQueue, _, _) =
        TrackedSourceQueue.createWithFlow[String, String](
          queue,
          trackerProbe.ref,
          ref = ref
        )

      trackerProbe.expectMsg(TrackerActor.RegisterRef(ref))

      trackedQueue
        .offer("elem-1")
        .futureValue shouldBe QueueOfferResult.Enqueued
      trackerProbe.expectMsg(TrackerActor.IncreaseQueueSize(ref))

      trackedQueue.offer("elem-2").futureValue shouldBe QueueOfferResult.Dropped

      trackerProbe.expectMsg(TrackerActor.ElementDropped(ref))

      downstreamProbe.request(1)
      downstreamProbe.expectNext("elem-1")
    }

    "delegate fail, complete, and watchCompletion to the underlying queue" in {
      val trackerProbe = new TestProbe(system)
      val ref          = UUID.randomUUID()
      val sourceWithQueue: Source[String, SourceQueueWithComplete[String]] =
        Source.queue[String](10, OverflowStrategy.dropHead)
      val (queue, doneFuture) =
        sourceWithQueue.toMat(Sink.ignore)(Keep.both).run()

      val (trackedQueue, _, _) =
        TrackedSourceQueue.createWithFlow[String, String](
          queue,
          trackerActor = trackerProbe.ref,
          ref          = ref
        )
      trackerProbe.expectMsg(TrackerActor.RegisterRef(ref))

      val watch = trackedQueue.watchCompletion()
      watch.isCompleted shouldBe false
      trackedQueue.complete()
      watch.futureValue shouldBe Done
      doneFuture.futureValue shouldBe Done

      intercept[TestFailedException] {
        trackedQueue.offer("another").futureValue
      }.message.mkString.contains(
        "stopped before"
      ) shouldBe true

      val sourceWithQueue2: Source[String, SourceQueueWithComplete[String]] =
        Source.queue[String](10, OverflowStrategy.dropHead)
      val queue2 = sourceWithQueue2.toMat(Sink.ignore)(Keep.left).run()
      val (trackedQueue2, _, _) =
        TrackedSourceQueue.createWithFlow[String, String](
          queue2,
          trackerProbe.ref,
          ref = ref
        )
      val ex = new RuntimeException("boom")
      trackedQueue2.fail(ex)
      intercept[TestFailedException] {
        trackedQueue2.offer("elem").futureValue
      }.message.mkString.contains(
        "DetachedException"
      ) shouldBe true

    }
  }

  "TrackedSourceQueue.createWithFlow" should {
    "create a trackingFlow that sends DecreaseQueueSize when condition is true" in {
      val trackerProbe = new TestProbe(system)
      val ref          = UUID.randomUUID()
      val sourceWithQueue: Source[String, SourceQueueWithComplete[String]] =
        Source.queue[String](10, OverflowStrategy.dropHead)
      val queue = sourceWithQueue.toMat(Sink.ignore)(Keep.left).run()

      val (_, trackingFlow, _) =
        TrackedSourceQueue.createWithFlow[String, String](
          queue,
          trackerProbe.ref,
          trackingCondition = _.startsWith("track"),
          ref               = ref
        )
      trackerProbe.expectMsg(TrackerActor.RegisterRef(ref))

      Source
        .single("track-me")
        .via(trackingFlow)
        .runWith(Sink.ignore)
        .futureValue
      trackerProbe.expectMsg(TrackerActor.DecreaseQueueSize(ref))

      Source
        .single("ignore-me")
        .via(trackingFlow)
        .runWith(Sink.ignore)
        .futureValue
      trackerProbe.expectNoMessage(200.millis)
    }
  }

  "TrackedSourceQueue and TrackerActor integration" should {
    "provide a stream that correctly reports queue size changes" in {
      val trackerActor = system.actorOf(
        Props[TrackerActor](),
        s"tracker-actor-${UUID.randomUUID()}"
      )
      val ref = UUID.randomUUID()
      val sourceWithQueue: Source[
        String,
        org.apache.pekko.stream.scaladsl.SourceQueueWithComplete[String]
      ] =
        Source.queue[String](10, OverflowStrategy.dropHead)
      val (queue, source) = sourceWithQueue.preMaterialize()

      val (trackedQueue, trackingFlow, getQueueSizeStream) =
        TrackedSourceQueue.createWithFlow[String, String](
          queue        = queue,
          trackerActor = trackerActor,
          ref          = ref
        )

      val sizeChangeProbe = getQueueSizeStream().runWith(TestSink.probe)
      val consumerProbe
          : org.apache.pekko.stream.testkit.TestSubscriber.Probe[String] =
        source.via(trackingFlow).runWith(TestSink.probe)

      // 1. Initial state
      sizeChangeProbe.request(1).expectNextPF { case (_, size) =>
        size shouldBe 0
      }

      // 2. Offer an element -> size becomes 1
      trackedQueue
        .offer("elem-1")
        .futureValue shouldBe QueueOfferResult.Enqueued
      sizeChangeProbe.request(1).expectNextPF { case (_, size) =>
        size shouldBe 1
      }

      // 3. Offer another element -> size becomes 2
      trackedQueue
        .offer("elem-2")
        .futureValue shouldBe QueueOfferResult.Enqueued
      sizeChangeProbe.request(1).expectNextPF { case (_, size) =>
        size shouldBe 2
      }

      // 4. Process an element -> size becomes 1
      consumerProbe.request(1).expectNext("elem-1")
      sizeChangeProbe.request(1).expectNextPF { case (_, size) =>
        size shouldBe 1
      }

      // 5. Process the final element -> size becomes 0
      consumerProbe.request(1).expectNext("elem-2")
      sizeChangeProbe.request(1).expectNextPF { case (_, size) =>
        size shouldBe 0
      }

      // 6. Clean up
      sizeChangeProbe.cancel()
      consumerProbe.cancel()
    }
  }

}
