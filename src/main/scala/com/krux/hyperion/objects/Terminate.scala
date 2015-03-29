package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpTerminate

class Terminate(
  implicit val hc: HyperionContext
) extends PipelineObject {

  val id = "TerminateAction"

  def serialize = new AdpTerminate()

}