package com.keivanabdi.datareeler.stream

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.ClosedShape
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl._

import com.keivanabdi.datareeler.models.ReelElement
import com.keivanabdi.datareeler.stream.GateStage
import org.joda.time.DateTime

object StreamPacer {

  def evenPacer[D, MD, TI, Mat1, Mat2](
      fastSource               : Source[ReelElement[D, MD, TI], Mat1],
      slowSource               : Source[DateTime, Mat2],
      slowSourceEmptinessSignal: Source[Boolean, ?]
  )(using Materializer): (
      Source[ReelElement[D, MD, TI], NotUsed],
      Source[DateTime, NotUsed]
  ) = {
    val gateStage =
      new GateStage[ReelElement[D, MD, TI]](initialSignalState = true)

    // Define the graph that processes elements from both sources
    val (fastSourcePublisher, slowSourcePublisher) =
      RunnableGraph
        .fromGraph(
          GraphDSL.createGraph(
            Sink.asPublisher[ReelElement[D, MD, TI]](
              false
            ), // Publisher for the fast source's processed elements
            Sink.asPublisher[DateTime](
              false
            ) // Publisher for the slow source's tracked elements
          )((matValue1, matValue2) => (matValue1, matValue2)) {
            implicit builder => (fastSourceOut, slowSourceOut) =>
              import GraphDSL.Implicits._

              // Partition the fast source elements based on the condition
              val fastSourcePartitioner = builder.add(
                Partition[ReelElement[D, MD, TI]](
                  outputPorts = 2,
                  partitioner = element =>
                    if (element.userData.isDefined) 0
                    else 1 // 0 for matched, 1 for unmatched
                )
              )

              // Zip matched fast source elements with slow source elements
              val fastSlowZipper =
                builder.add(Zip[ReelElement[D, MD, TI], DateTime]())

              // Broadcast the zipped elements to two paths
              val zippedElementsBroadcaster =
                builder.add(Broadcast[(ReelElement[D, MD, TI], DateTime)](2))

              // Extract the fast source element from the zipped pair (for merging)
              val matchedFastSourceExtractor =
                builder.add(Flow[(ReelElement[D, MD, TI], DateTime)].map(_._1))

              // Extract the slow source element from the zipped pair (for direct output)
              val slowSourceExtractor =
                builder.add(Flow[(ReelElement[D, MD, TI], DateTime)].map(_._2))

              // Merge the matched and unmatched fast source elements
              val merger =
                builder.add(Merge[ReelElement[D, MD, TI]](2))

              val gate =
                builder.add(gateStage)

              // Graph connections
              fastSource ~> fastSourcePartitioner.in
              fastSourcePartitioner.out(
                0
              ) ~> fastSlowZipper.in0          // Matched fast source elements to zip
              slowSource ~> fastSlowZipper.in1 // Slow source elements to zip
              fastSlowZipper.out ~> zippedElementsBroadcaster.in
              zippedElementsBroadcaster.out(0) ~> matchedFastSourceExtractor ~>
                merger.in(0) // Zipped fast source elements to merge
              zippedElementsBroadcaster.out(1) ~> slowSourceExtractor ~>
                slowSourceOut // Zipped slow source elements to output

              fastSourcePartitioner.out(
                1
              ) ~> gate.in0 // Unmatched fast source elements to gate
              slowSourceEmptinessSignal ~> gate.in1
              gate.out ~> merger.in(1)
              merger.out ~> fastSourceOut // Merged fast source elements to output

              ClosedShape
          }
        )
        .run()

    // Create sources from the publishers
    val fastSourceOutput = Source.fromPublisher(fastSourcePublisher)
    val slowSourceOutput = Source.fromPublisher(slowSourcePublisher)

    // Return the two output sources
    fastSourceOutput -> slowSourceOutput
  }

}
