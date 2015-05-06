package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpActivity, AdpRef}

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
