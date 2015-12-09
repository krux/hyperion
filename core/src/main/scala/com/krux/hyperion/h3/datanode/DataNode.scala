package com.krux.hyperion.h3.datanode

import shapeless._

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.{ AdpRef, AdpDataNode }
import com.krux.hyperion.h3.common.PipelineObject
import com.krux.hyperion.precondition.Precondition

trait DataNode extends PipelineObject {

  type Self <: DataNode

  def dataNodeFieldsLens: Lens[Self, DataNodeFields]

  def preconditions = (dataNodeFieldsLens >> 'precondition).get(self)
  def whenMet(conditions: Precondition*) =
    (dataNodeFieldsLens >> 'precondition).modify(self)(_ ++ conditions)


  def onFailAlarms = (dataNodeFieldsLens >> 'onFailAlarms).get(self)
  def onFail(alarms: SnsAlarm*): Self =
    (dataNodeFieldsLens >> 'onFailAlarms).modify(self)(_ ++ alarms)

  def onSuccessAlarms = (dataNodeFieldsLens >> 'onSuccessAlarms).get(self)
  def onSuccess(alarms: SnsAlarm*): Self =
    (dataNodeFieldsLens >> 'onSuccessAlarms).modify(self)(_ ++ alarms)

  lazy val ref: AdpRef[AdpDataNode] = AdpRef(serialize)

  def serialize: AdpDataNode

}
