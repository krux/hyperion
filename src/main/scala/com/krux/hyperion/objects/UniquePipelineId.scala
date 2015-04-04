package com.krux.hyperion.objects

import java.util.UUID

class UniquePipelineId(prefix: String) {

  val uniqueId = prefix + "_" + UUID.randomUUID.toString

  override def toString = uniqueId

}

class FixedPipelineId(fixedId: String) extends UniquePipelineId(fixedId) {
  override val uniqueId = fixedId
}

object ScheduleUniquePipelineId extends FixedPipelineId("PipelineSchedule")

object TerminateUniquePipelineId extends FixedPipelineId("TerminateAction")

object DefaultObjectId extends FixedPipelineId("Default")
