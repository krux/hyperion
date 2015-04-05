package com.krux.hyperion.objects

trait DataNode extends PipelineObject {

  def named(name: String): DataNode
  def groupedBy(group: String): DataNode

  def preconditions: Seq[Precondition]
  def onSuccessAlarms: Seq[SnsAlarm]
  def onFailAlarms: Seq[SnsAlarm]

  def whenMet(preconditions: Precondition*): DataNode
  def onSuccess(alarms: SnsAlarm*): DataNode
  def onFail(alarms: SnsAlarm*): DataNode
}
