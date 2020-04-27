package com.krux.hyperion

trait PipelineLifeCycle {

  def onUploaded(id: String, name: String, status: Status.Value)

  def onCreated(id: String, name: String, status: Status.Value)

}

object Status extends Enumeration {
  val SUCCESS, FAIL, SUCCESS_WITH_WARNINGS = Value
}