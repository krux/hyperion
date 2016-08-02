package com.krux.hyperion.examples

import com.typesafe.config.ConfigFactory

import com.krux.hyperion.workflow.WorkflowExpression
import com.krux.hyperion.{ DataPipelineDefGroup, Schedule, WorkflowKey, HyperionContext,
  HyperionCli }
import com.krux.hyperion.activity.ShellCommandActivity
import com.krux.hyperion.resource.Ec2Resource


object ExamplePipelineGroup extends DataPipelineDefGroup with HyperionCli {

  override implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

  def schedule: Schedule = Schedule.onceAtActivation

  def configGroups: Map[WorkflowKey, Iterable[Int]] = Map(
    (Option("1"), (0 until 5)),
    (Option("2"), (5 until 6)),
    (Option("3"), (6 until 10))
  )

  def workflows: Map[WorkflowKey, WorkflowExpression] = configGroups.mapValues { intValues =>
    val ec2 = Ec2Resource()
    intValues.map(i => ShellCommandActivity(s"echo $i")(ec2).named(s"act$i"))
  }

}
