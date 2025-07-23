package com.keivanabdi.datareeler.templates

import scalatags.Text.all._

import org.apache.pekko.http.scaladsl.model.ContentTypes
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.server._
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

import com.keivanabdi.datareeler.models.*
import com.keivanabdi.datareeler.templates.FullWidthInfiniteScroll.Instructions
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

class FullWidthInfiniteScroll[D, MD](
    val dataHtmlRenderer: Option[
      FullWidthInfiniteScroll.Instructions
    ] => D => HtmlRenderable,
    val metaHtmlRenderer: Option[
      FullWidthInfiniteScroll.Instructions
    ] => MD => HtmlRenderable,
    styleBlocks                             : Seq[CssRenderable],
    javascriptBlocks                        : Seq[JavascriptRenderable],
    defaultButtonText                       : String,
    previouslyRequestedItemsProcessedText   : String,
    previouslyRequestedItemsNotProcessedText: String,
    streamFinishedText                      : String,
    sendingSignalText                       : String,
    sendingSignalAnimationDuration          : Int = 2000,
    updatingButtonTextDuration              : Int = 1500,
    borderAnimationDuration                 : Int = 3000
) extends ReelerTemplate[D, MD, FullWidthInfiniteScroll.Instructions] {

  lazy val baseHtml: String =
    html(
      scalatags.Text.all.head(
        link(
          rel  := "stylesheet",
          href := "/static/web/templates/full-width-infinite-scroll/index.css"
        ),
        styleBlocks.map(_.renderAsRawElement())
      ),
      body(
        div(id := "items"),
        div(cls := "footer")(
          button(
            id := "load-more",
            attr(
              "data-sending-signal-duration"
            )                                := sendingSignalAnimationDuration,
            attr("data-sending-signal-text") := sendingSignalText,
            attr(
              "data-updating-button-text-duration"
            )                                      := updatingButtonTextDuration,
            attr("data-border-animation-duration") := borderAnimationDuration
          )(
            span(
              cls := "button-text",
              defaultButtonText
            )
          ),
          div(cls := "metadata"),
          javascriptBlocks.map(_.renderAsRawElement())
        ),
        script(
          src := "/static/web/templates/full-width-infinite-scroll/index.js"
        )
      )
    ).render

  override def basePageRoute: Route =
    complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, baseHtml))

  def renderReelElement(
      element                     : ReelElement[D, MD, FullWidthInfiniteScroll.Instructions],
      previousUserMetaData        : Option[MD],
      previousTemplateInstructions: Option[FullWidthInfiniteScroll.Instructions]
  ): RenderedReelElement = {
    val effectiveTemplateInstructions: Option[Instructions] =
      element.templateInstructions.orElse(previousTemplateInstructions)

    val effectiveMetaData: Option[MD] =
      element.userMetaData.orElse(previousUserMetaData)

    RenderedReelElement(
      userData = element.userData.map(
        dataHtmlRenderer(effectiveTemplateInstructions).andThen(_.render())
      ),
      userMetaData =
        if (
          effectiveTemplateInstructions != previousTemplateInstructions ||
          effectiveMetaData != previousUserMetaData
        ) {
          effectiveMetaData
            .map(metaHtmlRenderer(effectiveTemplateInstructions))
            .map(_.render())
        } else {
          None
        },
      templateInstructions = element.templateInstructions.map(_.asJson)
    )
  }

  def previouslyRequestedItemsProcessedInstruction
      : FullWidthInfiniteScroll.Instructions =
    FullWidthInfiniteScroll.Instructions(
      maybeText                             = Some(previouslyRequestedItemsProcessedText),
      makeButtonDisabled                    = false,
      finishStream                          = false,
      setTextAsAlternativeDefaultButtonText = true,
      pendingDemands                        = 0
    )

  def previouslyRequestedItemsNotProcessedInstruction(
      pendingDemands: Int
  ): FullWidthInfiniteScroll.Instructions =
    FullWidthInfiniteScroll.Instructions(
      maybeText                             = Some(previouslyRequestedItemsNotProcessedText),
      makeButtonDisabled                    = false,
      finishStream                          = false,
      setTextAsAlternativeDefaultButtonText = true,
      pendingDemands                        = pendingDemands
    )

  def streamFinishedInstruction: FullWidthInfiniteScroll.Instructions =
    FullWidthInfiniteScroll.Instructions(
      maybeText                             = Some(streamFinishedText),
      makeButtonDisabled                    = true,
      finishStream                          = true,
      setTextAsAlternativeDefaultButtonText = false,
      pendingDemands                        = 0
    )

}

object FullWidthInfiniteScroll {

  final case class Instructions(
      maybeText                            : Option[String],
      makeButtonDisabled                   : Boolean,
      finishStream                         : Boolean,
      setTextAsAlternativeDefaultButtonText: Boolean,
      pendingDemands                       : Int
  )

}
