package com.keivanabdi.datareeler.stream

import org.apache.pekko.stream._
import org.apache.pekko.stream.stage._

import org.slf4j.LoggerFactory

class GateStage[A](initialSignalState: Boolean)
    extends GraphStage[FanInShape2[A, Boolean, A]] {

  private val log = LoggerFactory.getLogger(getClass.getName)

  override val shape: FanInShape2[A, Boolean, A] =
    new FanInShape2[A, Boolean, A]("GateStage.shape")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) {

      private val dataIn   : Inlet[A]       = shape.in0
      private val signalsIn: Inlet[Boolean] = shape.in1
      private val dataOut  : Outlet[A]      = shape.out

      private var isGateOpen            : Boolean   = initialSignalState
      private var pendingElement        : Option[A] = None
      private var dataInSourceCompleted : Boolean   = false
      private var signalsSourceCompleted: Boolean   = false

      private def canPullDataIn: Boolean =
        !dataInSourceCompleted && !hasBeenPulled(dataIn) && !isClosed(dataIn)

      private def canPullSignal: Boolean =
        !signalsSourceCompleted && !hasBeenPulled(signalsIn) && !isClosed(
          signalsIn
        )

      private def canPushOutputOut: Boolean =
        isAvailable(dataOut)

      override def preStart(): Unit = {
        log.info("preStart: Initial gate state: {}.", isGateOpen)
        // Pull for the first signal. dataIn will be pulled based on demand & gate state.
        if (!hasBeenPulled(signalsIn)) {
          log.info("preStart: Pulling initial signal.")
          pull(signalsIn)
        }
        // Do not pull DataIn here. Let dataOut.onPull or signal change drive it.
      }

      setHandler(
        dataIn,
        new InHandler {
          override def onPush(): Unit = {
            val element = grab(dataIn)
            log.info(
              "DataIn: Received {}. Gate: {}. Pending before: {}",
              element,
              isGateOpen,
              pendingElement
            )

            if (isGateOpen) {
              if (isAvailable(dataOut)) {
                log.info(
                  "DataIn: Gate OPEN, downstream ready. Pushing {}.",
                  element
                )
                push(dataOut, element)
                if (canPullDataIn) {
                  log.info("DataIn: Pushed {}. Pulling next dataIn.", element)
                  pull(dataIn)
                }
              } else {
                log.info(
                  "DataIn: Gate OPEN, but downstream NOT ready. Storing {} as pending.",
                  element
                )
                pendingElement = Some(element)
                // Don't pull next dataIn until this pending one is pushed
              }
            } else { // Gate is CLOSED
              log.info(
                "DataIn: Gate CLOSED. Storing {} as pending. (This means gate closed after pull was issued)",
                element
              )
              pendingElement = Some(element)
            }
          }

          override def onUpstreamFinish(): Unit = {
            log.info("dataIn source completed.")
            dataInSourceCompleted = true
            tryCompleteOutputStream()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            log.error(
              "dataIn source failed: {}",
              ex.getMessage,
              ex
            )
            failStage(ex)
          }
        }
      )

      setHandler(
        signalsIn,
        new InHandler {
          override def onPush(): Unit = {
            val newSignal = grab(signalsIn)
            val oldSignal = isGateOpen
            isGateOpen = newSignal
            log.info(
              "Signal received: {} (was {}). Pending: {}",
              isGateOpen,
              oldSignal,
              pendingElement
            )

            if (isGateOpen && !oldSignal) { // Gate just opened
              log.info("Gate just opened.")
              tryPushPendingdataInAndPullNext()    // Centralized logic
            } else if (!isGateOpen && oldSignal) { // Gate just closed
              log.info("Gate just closed.")
              // If we were pulling dataIn, DataIn.onPush (if an element arrives)
              // will handle not pulling the *next* one and storing it as pending.
            }

            if (canPullSignal) {
              pull(signalsIn)
            }
          }

          override def onUpstreamFinish(): Unit = {
            log.info("Signals source completed.")
            signalsSourceCompleted = true
            tryCompleteOutputStream()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            log.error(
              "Signals source failed: {}",
              ex.getMessage,
              ex
            )
            failStage(ex)
          }
        }
      )

      setHandler(
        dataOut,
        new OutHandler {
          override def onPull(): Unit = {
            log.info(
              "DataOut: PULL received. Gate: {}. Pending: {}",
              isGateOpen,
              pendingElement
            )
            if (isGateOpen) {
              tryPushPendingdataInAndPullNext()
            } else {
              log.info("DataOut.onPull: Gate CLOSED. Cannot satisfy pull now.")
              // Demand is noted. When gate opens, signal handler will trigger push/pull.
            }
          }

          override def onDownstreamFinish(cause: Throwable): Unit = {
            log.info("Downstream finished: {}", cause)
            completeStage()
          }
        }
      )

      // Helper to attempt pushing a pending dataIn and then pulling the next if appropriate
      private def tryPushPendingdataInAndPullNext(): Unit = {
        pendingElement match {
          case Some(numToPush) =>
            if (isGateOpen && canPushOutputOut) {
              log.info("Pushing PENDING dataIn: {}.", numToPush)
              push(dataOut, numToPush)
              pendingElement = None
              // After pushing pending, if dataIn source not done and we haven't pulled, pull next
              if (canPullDataIn) {
                log.info("Pushed pending. Pulling next dataIn.")
                pull(dataIn)
              }
            } else {
              log.info(
                "tryPushPendingdataIn: Cannot push {}. Gate: {}, OutAvailable: {}",
                numToPush,
                isGateOpen,
                isAvailable(dataOut)
              )
            }
          case None => // No pending dataIn
            if (isGateOpen && canPushOutputOut && canPullDataIn) {
              log.info(
                "No pending, gate open, downstream ready. Pulling dataIn."
              )
              pull(dataIn)
            }
        }
        tryCompleteOutputStream()
      }

      private def tryCompleteOutputStream(): Unit = {
        pendingElement match {
          case None =>
            if (dataInSourceCompleted) {
              log.info(
                "Gate stage completing: dataIn source complete and no pending dataIn."
              )
              if (!isClosed(dataOut)) {
                complete(dataOut)
              }
            }
          case Some(pendingNum) =>
            if (
              dataInSourceCompleted && signalsSourceCompleted && !isGateOpen
            ) {
              log.warn(
                "Gate stage may not complete: dataIn & Signals complete, gate CLOSED, but PENDING dataIn {} will NOT be emitted.",
                pendingNum
              )
            }
        }
      }
    }
  }

}
