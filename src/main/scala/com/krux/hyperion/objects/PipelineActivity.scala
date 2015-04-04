package com.krux.hyperion.objects

trait PipelineActivity extends PipelineObject {

  def withName(name: String): PipelineActivity

  def dependsOn(activities: PipelineActivity*): PipelineActivity
  def whenMet(preconditions: Precondition*): PipelineActivity

  def onFail(alarms: SnsAlarm*): PipelineActivity
  def onSuccess(alarms: SnsAlarm*): PipelineActivity
  def onLateAction(alarms: SnsAlarm*): PipelineActivity

}
