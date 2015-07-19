package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.{AdpActivity, AdpRef}
import com.krux.hyperion.common.PipelineObject
import com.krux.hyperion.precondition.Precondition

/**
 * The activity trait. All activities should mixin this trait.
 */
trait PipelineActivity extends PipelineObject {

  def groupedBy(client: String): PipelineActivity
  def named(name: String): PipelineActivity

  private[hyperion] def dependsOn(activities: PipelineActivity*): PipelineActivity
  def dependsOn: Seq[PipelineActivity]
  def whenMet(conditions: Precondition*): PipelineActivity

  def onFail(alarms: SnsAlarm*): PipelineActivity
  def onSuccess(alarms: SnsAlarm*): PipelineActivity
  def onLateAction(alarms: SnsAlarm*): PipelineActivity

  def serialize: AdpActivity
  def ref: AdpRef[AdpActivity] = AdpRef(serialize)

}
