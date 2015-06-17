package com.krux.hyperion.examples

import com.krux.hyperion.activity.ShellCommandActivity
import com.krux.hyperion.Implicits._
import com.krux.hyperion.resource.Ec2Resource
import com.krux.hyperion.WorkflowDSL._
import com.krux.hyperion.{Schedule, DataPipelineDef, HyperionContext}
import com.typesafe.config.ConfigFactory

object ExampleWorkflow extends DataPipelineDef {

  override implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

  override lazy val schedule = Schedule()
    .startAtActivation
    .every(1.day)

  override def workflow = {

    val ec2 = Ec2Resource()

    // First activity
    val act1 = ShellCommandActivity(ec2).withCommand("run act1").named("act1")
    val act2 = ShellCommandActivity(ec2).withCommand("run act2").named("act2")
    val act3 = ShellCommandActivity(ec2).withCommand("run act3").named("act3")
    val act4 = ShellCommandActivity(ec2).withCommand("run act4").named("act4")
    val act5 = ShellCommandActivity(ec2).withCommand("run act5").named("act5")
    val act6 = ShellCommandActivity(ec2).withCommand("run act6").named("act6")

    // run act1 first, and then run act2 and act3 at the same time, and then run act4 and act5 the
    // same time, at last run act6
    // Anoternative syntax would be:
    // act1 andThen (act2 and act3) andThen (act4 and act5) andThen act6
    // or
    // act6 <~: (act4 + act5) <~: (act2 + act3) <~: act1
    act1 :~> (act2 + act3) :~> (act4 + act5) :~> act6

  }

}
