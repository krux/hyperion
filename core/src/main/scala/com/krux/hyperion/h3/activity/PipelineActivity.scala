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

  def activityFieldsLens: Lens[Self, ActivityFields[A]]

  def dependsOn = (activityFieldsLens >> 'dependsOn).get(self)
  private[hyperion] def dependsOn(activities: PipelineActivity[_]*): Self =
    (activityFieldsLens >> 'dependsOn).modify(self)(_ ++ activities)

  def preconditions = (activityFieldsLens >> 'preconditions).get(self)
  def whenMet(conditions: Precondition*): Self =
    (activityFieldsLens >> 'preconditions).modify(self)(_ ++ conditions)

  def onFailAlarms = (activityFieldsLens >> 'onFailAlarms).get(self)
  def onFail(alarms: SnsAlarm*): Self =
    (activityFieldsLens >> 'onFailAlarms).modify(self)(_ ++ alarms)

  def onSuccessAlarms = (activityFieldsLens >> 'onSuccessAlarms).get(self)
  def onSuccess(alarms: SnsAlarm*): Self =
    (activityFieldsLens >> 'onSuccessAlarms).modify(self)(_ ++ alarms)

  def onLateActionAlarms = (activityFieldsLens >> 'onLateActionAlarms).get(self)
  def onLateAction(alarms: SnsAlarm*): Self =
    (activityFieldsLens >> 'onLateActionAlarms).modify(self)(_ ++ alarms)

  def maximumRetries = (activityFieldsLens >> 'maximumRetries).get(self)
  def withMaximumRetries(retries: HInt): Self =
    (activityFieldsLens >> 'maximumRetries).set(self)(Option(retries))

  def attemptTimeout = (activityFieldsLens >> 'attemptTimeout).get(self)
  def withAttemptTimeout(duration: HDuration): Self =
    (activityFieldsLens >> 'attemptTimeout).set(self)(Option(duration))

  def lateAfterTimeout = (activityFieldsLens >> 'lateAfterTimeout).get(self)
  def withLateAfterTimeout(duration: HDuration): Self =
    (activityFieldsLens >> 'lateAfterTimeout).set(self)(Option(duration))

  def retryDelay = (activityFieldsLens >> 'retryDelay).get(self)
  def withRetryDelay(duration: HDuration): Self =
    (activityFieldsLens >> 'retryDelay).set(self)(Option(duration))

  def failureAndRerunMode = (activityFieldsLens >> 'failureAndRerunMode).get(self)
  def withFailureAndRerunMode(mode: FailureAndRerunMode): Self =
    (activityFieldsLens >> 'failureAndRerunMode).set(self)(Option(mode))

  // TODO: Uncomment the following once the other activities has been transformed
  def objects: Iterable[PipelineObject] = dependsOn // ++
    // activityFields.onFailAlarms ++
    // activityFields.onSuccessAlarms ++
    // activityFields.onLateActionAlarms ++
    // activityFields.preconditions :+
    // activityFields.runsOn

  def runsOn: Resource[A] = (activityFieldsLens >> 'runsOn).get(self)

  def serialize: AdpActivity
  def ref: AdpRef[AdpActivity] = AdpRef(serialize)

}
