package com.keivanabdi.datareeler.models

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

final case class RenderedReelElement(
    userData            : Option[String],
    userMetaData        : Option[String],
    templateInstructions: Option[Json]
) {
  def isEmpty: Boolean =
    userData.isEmpty && userMetaData.isEmpty && templateInstructions.isEmpty
}
