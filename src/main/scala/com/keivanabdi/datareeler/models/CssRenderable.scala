package com.keivanabdi.datareeler.models

import scalatags.Text.RawFrag
import scalatags.Text.all.{style as _, *}
import scalatags.Text.tags2._

trait CssRenderable extends Renderable {
  def renderAsRawElement(): RawFrag = raw(render())

}

object CssRenderable {

  final case class RawStyle(cssCode: String) extends CssRenderable {
    def render(): String = style(raw(cssCode)).render

  }

  final case class CssFile(path: String) extends CssRenderable {
    def render(): String = link(rel := "stylesheet", href := path).render
  }

}
