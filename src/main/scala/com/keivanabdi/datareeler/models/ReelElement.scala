package com.keivanabdi.datareeler.models

final case class ReelElement[D, MD, TI](
    userData            : Option[D],
    userMetaData        : Option[MD] = None,
    templateInstructions: Option[TI] = None
) {

  def isEmpty: Boolean =
    userData.isEmpty && userMetaData.isEmpty && templateInstructions.isEmpty

}
