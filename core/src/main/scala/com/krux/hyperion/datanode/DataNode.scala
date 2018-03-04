package com.krux.hyperion.datanode

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.{AdpRef, AdpDataNode}
import com.krux.hyperion.common.NamedPipelineObject
import com.krux.hyperion.precondition.Precondition


trait DataNode extends NamedPipelineObject {

  type Self <: DataNode

  def dataNodeFields: DataNodeFields
  def updateDataNodeFields(fields: DataNodeFields): Self

  def preconditions = dataNodeFields.preconditions
  def whenMet(conditions: Precondition*) = updateDataNodeFields(
    dataNodeFields.copy(preconditions = dataNodeFields.preconditions ++ conditions)
  )

  def onFailAlarms = dataNodeFields.onFailAlarms
  def onFail(alarms: SnsAlarm*): Self = updateDataNodeFields(
    dataNodeFields.copy(onFailAlarms = dataNodeFields.onFailAlarms ++ alarms)
  )

  def onSuccessAlarms = dataNodeFields.onSuccessAlarms
  def onSuccess(alarms: SnsAlarm*): Self = updateDataNodeFields(
    dataNodeFields.copy(onSuccessAlarms = dataNodeFields.onSuccessAlarms ++ alarms)
  )

  lazy val ref: AdpRef[AdpDataNode] = AdpRef(serialize)

  def serialize: AdpDataNode

  def objects = preconditions ++ onFailAlarms ++ onSuccessAlarms

}
