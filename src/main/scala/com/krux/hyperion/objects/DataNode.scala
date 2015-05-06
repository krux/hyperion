package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpDataNode, AdpRef}

trait DataNode extends PipelineObject {

  def named(name: String): DataNode
  def groupedBy(group: String): DataNode

  def preconditions: Seq[Precondition]
  def onSuccessAlarms: Seq[SnsAlarm]
  def onFailAlarms: Seq[SnsAlarm]

  def whenMet(conditions: Precondition*): DataNode
  def onSuccess(alarms: SnsAlarm*): DataNode
  def onFail(alarms: SnsAlarm*): DataNode

  def serialize: AdpDataNode
  def ref: AdpRef[AdpDataNode] = AdpRef(serialize)
}
