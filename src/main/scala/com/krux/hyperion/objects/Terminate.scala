package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpTerminate

object Terminate extends PipelineObject {

  val id = TerminateUniquePipelineId

  def serialize = new AdpTerminate()

}
