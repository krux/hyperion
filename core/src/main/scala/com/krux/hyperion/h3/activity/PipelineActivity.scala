package com.krux.hyperion.h3.activity

import shapeless._

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
  def activityFieldsLens: Lens[Self, ActivityFields[A]]

  def dependsOn = activityFields.dependsOn
  private[hyperion] def dependsOn(activities: PipelineActivity[_]*): Self =
    (activityFieldsLens >> 'dependsOn).modify(self)(_ ++ activities)

  def preconditions = activityFields.preconditions
  def whenMet(conditions: Precondition*): Self =
    (activityFieldsLens >> 'preconditions).modify(self)(_ ++ conditions)

  def onFailAlarms = activityFields.onFailAlarms
  def onFail(alarms: SnsAlarm*): Self =
    (activityFieldsLens >> 'onFailAlarms).modify(self)(_ ++ alarms)

  def onSuccessAlarms = activityFields.onSuccessAlarms
  def onSuccess(alarms: SnsAlarm*): Self =
    (activityFieldsLens >> 'onSuccessAlarms).modify(self)(_ ++ alarms)

  def onLateActionAlarms = activityFields.onLateActionAlarms
  def onLateAction(alarms: SnsAlarm*): Self =
    (activityFieldsLens >> 'onLateActionAlarms).modify(self)(_ ++ alarms)

  def maximumRetries = activityFields.maximumRetries
  def withMaximumRetries(retries: HInt): Self =
    (activityFieldsLens >> 'maximumRetries).set(self)(Option(retries))

  def attemptTimeout = activityFields.attemptTimeout
  def withAttemptTimeout(duration: HDuration): Self =
    (activityFieldsLens >> 'attemptTimeout).set(self)(Option(duration))

  def lateAfterTimeout = activityFields.lateAfterTimeout
  def withLateAfterTimeout(duration: HDuration): Self =
    (activityFieldsLens >> 'lateAfterTimeout).set(self)(Option(duration))

  def retryDelay = activityFields.retryDelay
  def withRetryDelay(duration: HDuration): Self =
    (activityFieldsLens >> 'retryDelay).set(self)(Option(duration))

  def failureAndRerunMode = activityFields.failureAndRerunMode
  def withFailureAndRerunMode(mode: FailureAndRerunMode): Self =
    (activityFieldsLens >> 'failureAndRerunMode).set(self)(Option(mode))

  // TODO: Uncomment the following once the other activities has been transformed
  def objects: Iterable[PipelineObject] = activityFields.dependsOn // ++
    // activityFields.onFailAlarms ++
    // activityFields.onSuccessAlarms ++
    // activityFields.onLateActionAlarms ++
    // activityFields.preconditions :+
    // activityFields.runsOn

  def runsOn: Resource[A] = activityFields.runsOn

  def serialize: AdpActivity
  def ref: AdpRef[AdpActivity] = AdpRef(serialize)

}
