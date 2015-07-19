package com.krux.hyperion.datanode

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.{AdpDataNode, AdpRef}
import com.krux.hyperion.common.PipelineObject
import com.krux.hyperion.precondition.Precondition

/**
 * The base trait of all data nodes
 */
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
