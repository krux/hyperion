package com.krux.hyperion.objects

trait DataNode extends PipelineObject {
  def preconditions: Seq[Precondition]
  def onSuccessAlarms: Seq[SnsAlarm]
  def onFailAlarms: Seq[SnsAlarm]

  def whenMet(preconditions: Precondition*): DataNode
  def onSuccess(alarms: SnsAlarm*): DataNode
  def onFail(alarms: SnsAlarm*): DataNode
}