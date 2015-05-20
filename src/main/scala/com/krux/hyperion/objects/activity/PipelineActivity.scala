package com.krux.hyperion.objects.activity

import com.krux.hyperion.objects.action.SnsAlarm
import com.krux.hyperion.objects.aws.{AdpActivity, AdpRef}
import com.krux.hyperion.objects.precondition.Precondition
import com.krux.hyperion.objects.PipelineObject

/**
 * The activity trait. All activities should mixin this trait.
 */
trait PipelineActivity extends PipelineObject {

  def groupedBy(client: String): PipelineActivity
  def named(name: String): PipelineActivity

  def dependsOn(activities: PipelineActivity*): PipelineActivity
  def whenMet(conditions: Precondition*): PipelineActivity

  def onFail(alarms: SnsAlarm*): PipelineActivity
  def onSuccess(alarms: SnsAlarm*): PipelineActivity
  def onLateAction(alarms: SnsAlarm*): PipelineActivity

  def serialize: AdpActivity
  def ref: AdpRef[AdpActivity] = AdpRef(serialize)

}
