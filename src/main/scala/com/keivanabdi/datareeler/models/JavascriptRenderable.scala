package com.keivanabdi.datareeler.models

import scalatags.Text.RawFrag
import scalatags.Text.all._

trait JavascriptRenderable extends Renderable {
  def renderAsRawElement(): RawFrag = raw(render())
}

object JavascriptRenderable {

  final case class RawJavascript(jsCode: String) extends JavascriptRenderable {
    def render(): String = script(raw(jsCode)).render

  }

  final case class JavascriptFile(path: String) extends JavascriptRenderable {
    def render(): String = script(src := path).render
  }

}
