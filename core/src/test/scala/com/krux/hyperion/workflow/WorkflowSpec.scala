package com.krux.hyperion.workflow

import com.krux.hyperion.DataPipelineDef
import com.krux.hyperion.DataPipelineDef._
import com.krux.hyperion.Schedule.onceAtActivation
import com.krux.hyperion.activity.Script.string2Script
import com.krux.hyperion.activity.ShellCommandActivity
import com.krux.hyperion.resource.WorkerGroup
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class WorkflowSpec extends FlatSpec {
  it should "not require logUri" in {
    object NoConfigWorkflow extends DataPipelineDef {
      override def schedule = onceAtActivation
      override def workflow = ShellCommandActivity("hello world")(WorkerGroup("foo"))
    }

    noException should be thrownBy dataPipelineDef2Json(NoConfigWorkflow)
  }
}
