package com.krux.hyperion

import com.krux.hyperion.common.Status

trait PipelineLifeCycle {

  def onCreated(id: String, name: String, status: Status.Value): Unit = {
  }

  def onUploaded(id: String, name: String, status: Status.Value): Unit = {
  }

}