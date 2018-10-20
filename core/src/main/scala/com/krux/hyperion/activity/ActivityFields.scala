package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.{ HInt, HDuration }
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{ ResourceObject, Resource }

case class ActivityFields[A <: ResourceObject](
  runsOn: Resource[A],
  dependsOn: Seq[PipelineActivity[_ <: ResourceObject]] = Seq.empty,
  preconditions: Seq[Precondition] = Seq.empty,
  onFailAlarms: Seq[SnsAlarm] = Seq.empty,
  onSuccessAlarms: Seq[SnsAlarm] = Seq.empty,
  onLateActionAlarms: Seq[SnsAlarm] = Seq.empty,
  maximumRetries: Option[HInt] = None,
  attemptTimeout: Option[HDuration] = None,
  lateAfterTimeout: Option[HDuration] = None,
  retryDelay: Option[HDuration] = None,
  failureAndRerunMode: Option[FailureAndRerunMode] = None,
  maxActiveInstances: Option[HInt] = None
)
