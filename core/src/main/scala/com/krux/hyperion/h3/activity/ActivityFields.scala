package com.krux.hyperion.h3.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.{ HInt, HDuration, HString, HBoolean }
import com.krux.hyperion.h3.resource.{ ResourceObject, Resource }
import com.krux.hyperion.precondition.Precondition

/**
 * TODO: with this implementation workerGroup is not supported.
 */
case class ActivityFields[A <: ResourceObject](
  runsOn: Resource[A],
  dependsOn: Seq[PipelineActivity[_]] = Seq.empty,
  preconditions: Seq[Precondition] = Seq.empty,
  onFailAlarms: Seq[SnsAlarm] = Seq.empty,
  onSuccessAlarms: Seq[SnsAlarm] = Seq.empty,
  onLateActionAlarms: Seq[SnsAlarm] = Seq.empty,
  maximumRetries: Option[HInt] = None,
  attemptTimeout: Option[HDuration] = None,
  lateAfterTimeout: Option[HDuration] = None,
  retryDelay: Option[HDuration] = None,
  failureAndRerunMode: Option[FailureAndRerunMode] = None
)
