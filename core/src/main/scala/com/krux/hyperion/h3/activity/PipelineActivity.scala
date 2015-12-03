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

  private[hyperion] def dependsOn(activities: PipelineActivity[_]*): PipelineActivity[A] =
    (activityFieldsLens >> 'dependsOn).modify(self)(_ ++ activities)

  def whenMet(conditions: Precondition*): PipelineActivity[A] =
    (activityFieldsLens >> 'preconditions).modify(self)(_ ++ conditions)

  def onFail(alarms: SnsAlarm*): Self =
    (activityFieldsLens >> 'onFailAlarms).modify(self)(_ ++ alarms)

  def onSuccess(alarms: SnsAlarm*): Self =
    (activityFieldsLens >> 'onSuccessAlarms).modify(self)(_ ++ alarms)

  def onLateAction(alarms: SnsAlarm*): Self =
    (activityFieldsLens >> 'onLateActionAlarms).modify(self)(_ ++ alarms)

  def withMaximumRetries(retries: HInt): Self =
    (activityFieldsLens >> 'maximumRetries).set(self)(Option(retries))

  def withAttemptTimeout(duration: HDuration): Self =
    (activityFieldsLens >> 'attemptTimeout).set(self)(Option(duration))

  def withLateAfterTimeout(duration: HDuration): Self =
    (activityFieldsLens >> 'lateAfterTimeout).set(self)(Option(duration))

  def withRetryDelay(duration: HDuration): Self =
    (activityFieldsLens >> 'retryDelay).set(self)(Option(duration))

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
