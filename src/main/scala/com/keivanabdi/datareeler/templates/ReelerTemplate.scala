package com.keivanabdi.datareeler.templates

import org.apache.pekko.http.scaladsl.server.Route

import com.keivanabdi.datareeler.models._

trait ReelerTemplate[D, MD, TI] {
  val dataHtmlRenderer: Option[TI] => D => HtmlRenderable
  val metaHtmlRenderer: Option[TI] => MD => HtmlRenderable

  def basePageRoute: Route

  def renderReelElement(
      element                 : ReelElement[D, MD, TI],
      lastUserMetaData        : Option[MD],
      lastTemplateInstructions: Option[TI]
  ): RenderedReelElement

  def previouslyRequestedItemsProcessedInstruction                        : TI
  def previouslyRequestedItemsNotProcessedInstruction(pendingDemands: Int): TI
  def streamFinishedInstruction                                           : TI

}
