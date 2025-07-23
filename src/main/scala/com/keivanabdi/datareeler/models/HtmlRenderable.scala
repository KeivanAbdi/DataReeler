package com.keivanabdi.datareeler.models

sealed trait HtmlRenderable extends Renderable

object HtmlRenderable {

  final case class RawHtml(html: String) extends HtmlRenderable {
    def render(): String = html
  }

  final case class ScalatagsHtml(html: scalatags.Text.TypedTag[String])
      extends HtmlRenderable {
    def render(): String = html.render
  }

}
