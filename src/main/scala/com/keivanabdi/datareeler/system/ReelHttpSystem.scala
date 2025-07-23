package com.keivanabdi.datareeler.system

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.marshalling.sse.EventStreamMarshalling.*
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent
import org.apache.pekko.http.scaladsl.server.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future
import scala.concurrent.duration._

import com.keivanabdi.datareeler.models.ReelerSystemConfig
import com.keivanabdi.datareeler.templates.ReelerTemplate
import org.slf4j.LoggerFactory

class ReelHttpSystem[D, MD, TI](
    userRoutes: Seq[Route],
    config    : ReelerSystemConfig[D, MD, TI]
)(using
    actorSystem: ActorSystem
) {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  import actorSystem.dispatcher

  // Handles unhandled exceptions during request processing
  private val exceptionHandler: ExceptionHandler = ExceptionHandler {
    case e: Exception =>
      extractUri { uri =>
        logger.error(
          s"Unhandled exception during request processing for $uri: ${e.getMessage}",
          e
        )
        complete(
          org.apache.pekko.http.scaladsl.model.StatusCodes.InternalServerError ->
            "Internal server error"
        )
      }
  }

  // Defines control endpoints, prepending them to user-defined routes
  private def controlEndPoints(
      loadMoreSignal: () => Unit
  ): Seq[Route] =
    path("load-more") {
      loadMoreSignal()
      complete("")
    } +: userRoutes

  /** Starts the HTTP server and binds the routes.
    *
    * @param loadMoreSignal
    *   Function to trigger loading more data.
    * @param serverSentStream
    *   Function to provide the Server-Sent Events source.
    * @return
    *   A Future that completes with the ServerBinding.
    */
  def start(
      loadMoreSignal  : () => Unit,
      serverSentStream: () => Source[ServerSentEvent, NotUsed]
  ): Future[ServerBinding] = {
    val route: Route =
      handleExceptions(exceptionHandler) {
        concat(
          (
            Seq(
              pathEndOrSingleSlash {
                config.reelerTemplate.basePageRoute
              },
              path("events") {
                complete {
                  serverSentStream()
                }
              }
            ) ++ controlEndPoints(loadMoreSignal) :+
              pathPrefix("static") {
                extractUnmatchedPath { path =>
                  getFromResource(path.toString.stripPrefix("/"))
                }
              }
          )*
        )
      }

    Http()
      .newServerAt(config.interface, config.port)
      .bind(route)
      .map(
        _.addToCoordinatedShutdown(
          hardTerminationDeadline = 10.seconds
        )
      )
  }

}
