package com.krux.hyperion

import com.krux.hyperion.PipelineLifeCycle.Status

trait PipelineLifeCycle {

  def onCreated(id: String, name: String, status: Status.Value): Unit = {
  }

  def onUploaded(id: String, name: String, status: Status.Value): Unit = {
  }

}

object PipelineLifeCycle {
  object Status extends Enumeration {
    val Success, Fail, SuccessWithWarnings = Value
  }
}
