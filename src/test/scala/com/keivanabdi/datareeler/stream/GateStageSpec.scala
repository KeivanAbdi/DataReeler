package com.keivanabdi.datareeler.stream

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.ClosedShape
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl._
import org.apache.pekko.stream.scaladsl.{GraphDSL, RunnableGraph, Source}
import org.apache.pekko.stream.testkit.TestPublisher
import org.apache.pekko.stream.testkit.TestSubscriber
import org.apache.pekko.stream.testkit.scaladsl.{TestSink, TestSource}
import org.apache.pekko.testkit.TestKit

import scala.concurrent.Await
import scala.concurrent.duration._

import com.keivanabdi.datareeler.stream.GateStage
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class GateStageSpec
    extends TestKit(ActorSystem("GateStageSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  given materializer: Materializer = Materializer(system)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def setupTestGraph(initialSignalState: Boolean): (
      TestPublisher.Probe[Int],
      TestPublisher.Probe[Boolean],
      TestSubscriber.Probe[Int]
  ) = {
    val numberProducer = TestSource.probe[Int]
    val signalProducer = TestSource.probe[Boolean]
    val consumer       = TestSink.probe[Int]

    val graph =
      RunnableGraph.fromGraph(
        GraphDSL.createGraph(numberProducer, signalProducer, consumer)(
          (_, _, _)
        ) { implicit builder => (numbersSrcShape, signalsSrcShape, sinkShape) =>
          import GraphDSL.Implicits._

          val gate = builder.add(
            new GateStage[Int](initialSignalState = initialSignalState)
          )

          numbersSrcShape ~> gate.in0
          signalsSrcShape ~> gate.in1
          gate.out ~> sinkShape

          ClosedShape
        }
      )
    graph.run()
  }

  "A GateStage" should {

    "pass numbers when gate is initially open" in {
      val (numbers, signals, out) = setupTestGraph(initialSignalState = true)

      out.request(1)
      numbers.sendNext(1)
      out.expectNext(1)

      out.request(1)
      numbers.sendNext(2)
      out.expectNext(2)

      signals.sendNext(true) // Redundant signal

      out.request(1)
      numbers.sendNext(3)
      out.expectNext(3)

      numbers.sendComplete()
      out.expectComplete()
      signals.sendComplete()
    }

    "block numbers when initially closed, then open" in {
      val (numbers, signals, out) = setupTestGraph(initialSignalState = false)

      out.request(1)
      numbers.sendNext(10)
      out.expectNoMessage(200.millis) // Should be blocked

      signals.sendNext(true)          // Open the gate
      out.expectNext(1000.millis, 10) // Wait a bit longer, then expect

      out.request(1)
      numbers.sendNext(11)
      out.expectNext(11)

      numbers.sendComplete()
      out.expectComplete()
      signals.sendComplete()
    }

    "pass, block on gate close, pass on gate re-open" in {
      val (numbers, signals, out) = setupTestGraph(initialSignalState = true)

      out.request(1)
      numbers.sendNext(100)
      out.expectNext(100)

      signals.sendNext(false) // Close gate
      out.request(1)
      numbers.sendNext(101)
      out.expectNoMessage(200.millis) // Blocked

      numbers.sendNext(
        102
      )                               // Send another while closed (will be backpressured upstream)
      out.expectNoMessage(100.millis) // Still blocked

      signals.sendNext(true)          // Open gate
      out.expectNext(500.millis, 101) // Element that was sent and then blocked

      out.request(1)
      out.expectNext(500.millis, 102)

      out.request(1)
      numbers.sendNext(103)
      out.expectNext(103)

      numbers.sendComplete()
      out.expectComplete()
      signals.sendComplete()
    }

    "handle stuck element when numbers complete and gate is closed" in {
      val (numbers, signals, out) =
        setupTestGraph(initialSignalState = true) // Start open

      out.request(1)
      numbers.sendNext(201)
      out.expectNext(201)

      // Create a pending element: send it to numbersIn, but don't request from out yet.
      // The stage's numbersIn.onPush will be called. If downstream isn't ready (no out.request),
      // it should become pending.
      numbers.sendNext(202) // This is sent to the GateStage's numbersIn
      // At this point, 202 is either pending in GateStage, or TestSource is holding it if gate didn't pull.
      // GateStage's pull logic in onPush of numbersIn or onPull of dataOut is key here.
      // If GateStage pulled for 202 (after 201 was pushed), 202 will be in pendingNumber.
      out.expectNoMessage(100.millis) // Make sure 202 isn't pushed yet

      signals.sendNext(false) // Close the gate

      numbers.sendComplete() // Numbers source completes

      out.expectNoMessage(200.millis) // Should not emit 202, nor complete

      // Try requesting, should still be stuck
      out.request(1)
      out.expectNoMessage(100.millis)

      signals.sendComplete()          // Signals complete
      out.expectNoMessage(100.millis) // Still stuck

      out.cancel() // Clean up the sink to allow termination
    }
  }

  "fail stage if numbers source fails" in {
    val (numbers, signals, out) = setupTestGraph(initialSignalState = true)
    out.request(1)
    numbers.sendError(new RuntimeException("Numbers boom!"))
    out.expectError().getMessage should be("Numbers boom!")
    signals.sendComplete() // Or signals.cancel()
  }

  "fail stage if signals source fails" in {
    val (numbers, signals, out) = setupTestGraph(initialSignalState = true)
    out.request(1)
    signals.sendError(new RuntimeException("Signals boom!"))
    out.expectError().getMessage should be("Signals boom!")
    numbers.sendComplete() // Or numbers.cancel()
  }

  "respect first signal even if initial state differs and no numbers processed" in {
    val (numbers, signals, out) =
      setupTestGraph(initialSignalState = true) // Initially open

    signals.sendNext(false) // Immediately send a close signal

    out.request(1)
    numbers.sendNext(1)
    out.expectNoMessage(100.millis) // Should be blocked by the new signal

    signals.sendNext(true) // Open again
    out.expectNext(100.millis, 1)

    numbers.sendComplete()
    signals.sendComplete()
    out.expectComplete()
  }

  "handle rapid signal changes before number processing" in {
    val (numbers, signals, out) =
      setupTestGraph(initialSignalState = false) // Start closed

    signals.sendNext(true)  // Open
    signals.sendNext(false) // Close
    signals.sendNext(true)  // Open again (final state is open)

    out.request(1)
    numbers.sendNext(1)
    out.expectNext(100.millis, 1)

    numbers.sendComplete()
    signals.sendComplete()
    out.expectComplete()
  }

}
