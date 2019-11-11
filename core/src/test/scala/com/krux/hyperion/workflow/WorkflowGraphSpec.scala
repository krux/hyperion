package com.krux.hyperion.workflow

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.activity.ShellCommandActivity
import com.krux.hyperion.resource.Ec2Resource
import com.typesafe.config.ConfigFactory
import org.scalatest.{ Matchers, WordSpec }

class WorkflowGraphSpec extends WordSpec  with Matchers{

  "WorkflowGraph" should {

    implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

    val ec2 = Ec2Resource()

    "produce correct dependencies" in {

      val act1 = ShellCommandActivity("run act1")(ec2).idNamed("act1")
      val act2 = ShellCommandActivity("run act2")(ec2).idNamed("act2")
      val act3 = ShellCommandActivity("run act3")(ec2).idNamed("act3")
      val act4 = ShellCommandActivity("run act4")(ec2).idNamed("act4")
      val act5 = ShellCommandActivity("run act5")(ec2).idNamed("act5")
      val act6 = ShellCommandActivity("run act6")(ec2).idNamed("act6")

      val activityOrders = Seq(
        (act1 -> act3),
        (act2 -> act3),
        (act2 -> act4),
        (act3 -> act5),
        (act3 -> act6),
        (act4 -> act6)
      )

      val workflowGraph = activityOrders.foldLeft(WorkflowGraph()) { case (g, (a1, a2)) =>
        g ++ (WorkflowGraph(a1) ~> WorkflowGraph(a2))
      }

      val rAct3 = act3.dependsOn(act1, act2)
      val rAct4 = act4.dependsOn(act2)
      val rAct5 = act5.dependsOn(rAct3)
      val rAct6 = act6.dependsOn(rAct3, rAct4)

      workflowGraph.toActivities should contain theSameElementsAs Seq(
        act1, act2, rAct3, rAct4, rAct5, rAct6
      )
    }
  }
}
