package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.{ HInt, HDuration }
import com.krux.hyperion.aws.{ AdpActivity, AdpRef }
import com.krux.hyperion.common.{ PipelineObject, NamedPipelineObject }
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{ Resource, ResourceObject }

/**
 * The activity trait. All activities should mixin this trait.
 */
trait PipelineActivity[A <: ResourceObject] extends NamedPipelineObject {

  type Self <: PipelineActivity[A]

  def activityFields: ActivityFields[A]
  def updateActivityFields(fields: ActivityFields[A]): Self

  def dependsOn = activityFields.dependsOn
  private[hyperion] def dependsOn(activities: PipelineActivity[_]*): Self = updateActivityFields(
    activityFields.copy(dependsOn = activityFields.dependsOn ++ activities)
  )

  def preconditions = activityFields.preconditions
  def whenMet(conditions: Precondition*): Self = updateActivityFields(
    activityFields.copy(preconditions = activityFields.preconditions ++ conditions)
  )

  def onFailAlarms = activityFields.onFailAlarms
  def onFail(alarms: SnsAlarm*): Self = updateActivityFields(
    activityFields.copy(onFailAlarms = activityFields.onFailAlarms ++ alarms)
  )

  def onSuccessAlarms = activityFields.onSuccessAlarms
  def onSuccess(alarms: SnsAlarm*): Self = updateActivityFields(
    activityFields.copy(onSuccessAlarms = activityFields.onSuccessAlarms ++ alarms)
  )

  def onLateActionAlarms = activityFields.onLateActionAlarms
  def onLateAction(alarms: SnsAlarm*): Self = updateActivityFields(
    activityFields.copy(onLateActionAlarms = activityFields.onLateActionAlarms ++ alarms)
  )

  def maximumRetries = activityFields.maximumRetries
  def withMaximumRetries(retries: HInt): Self = updateActivityFields(
    activityFields.copy(maximumRetries = Option(retries))
  )

  def attemptTimeout = activityFields.attemptTimeout
  def withAttemptTimeout(duration: HDuration): Self = updateActivityFields(
    activityFields.copy(attemptTimeout = Option(duration))
  )

  def lateAfterTimeout = activityFields.lateAfterTimeout
  def withLateAfterTimeout(duration: HDuration): Self = updateActivityFields(
    activityFields.copy(lateAfterTimeout = Option(duration))
  )

  def retryDelay = activityFields.retryDelay
  def withRetryDelay(duration: HDuration): Self = updateActivityFields(
    activityFields.copy(retryDelay = Option(duration))
  )

  def failureAndRerunMode = activityFields.failureAndRerunMode
  def withFailureAndRerunMode(mode: FailureAndRerunMode): Self = updateActivityFields(
    activityFields.copy(failureAndRerunMode = Option(mode))
  )

  def maxActiveInstances = activityFields.maxActiveInstances
  def withMaxActiveInstances(activeInstances: HInt): Self = updateActivityFields(
    activityFields.copy(maxActiveInstances = Option(activeInstances))
  )

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ dependsOn ++
    activityFields.onFailAlarms ++
    activityFields.onSuccessAlarms ++
    activityFields.onLateActionAlarms ++
    activityFields.preconditions

  def runsOn: Resource[A] = activityFields.runsOn

  def serialize: AdpActivity
  def ref: AdpRef[AdpActivity] = AdpRef(serialize)

}
