package com.krux.hyperion.h3.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.{HInt, HDuration, HString, HBoolean}
import com.krux.hyperion.aws.{AdpActivity, AdpRef}
import com.krux.hyperion.h3.common.PipelineObject
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.activity.FailureAndRerunMode
import com.krux.hyperion.resource.{ Resource, ResourceObject }

/**
 * The activity trait. All activities should mixin this trait.
 */
trait PipelineActivity[A <: ResourceObject] extends PipelineObject {

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

  // TODO: Uncomment the following once the other activities has been transformed
  def objects: Iterable[PipelineObject] = dependsOn // ++
    // activityFields.onFailAlarms ++
    // activityFields.onSuccessAlarms ++
    // activityFields.onLateActionAlarms ++
    // activityFields.preconditions :+
    // activityFields.runsOn

  def runsOn: Resource[A] = activityFields.runsOn

  def serialize: AdpActivity
  def ref: AdpRef[AdpActivity] = AdpRef(serialize)

}
