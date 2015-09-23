package com.krux.hyperion.workflow

import com.krux.hyperion.activity.ShellCommandActivity
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.resource.Ec2Resource
import com.krux.hyperion.WorkflowExpression._
import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec

class WorkflowGraphSpec extends WordSpec {

  "WorkflowGraph" should {

    implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

    val ec2 = Ec2Resource()

    "produce correct dependencies" in {

      val act1 = ShellCommandActivity("run act1")(ec2).named("act1")
      val act2 = ShellCommandActivity("run act2")(ec2).named("act2")
      val act3 = ShellCommandActivity("run act3")(ec2).named("act3")
      val act4 = ShellCommandActivity("run act4")(ec2).named("act4")
      val act5 = ShellCommandActivity("run act5")(ec2).named("act5")
      val act6 = ShellCommandActivity("run act6")(ec2).named("act6")

      val activityOrders = Seq(
        (act1 -> act3),
        (act2 -> act3),
        (act2 -> act4),
        (act3 -> act5),
        (act3 -> act6),
        (act4 -> act6)
      )

      val workflowGraph = activityOrders.foldLeft(new WorkflowGraph()) { case (g, (a1, a2)) =>
        g + (a1, a2)
      }

      val acts = workflowGraph.toActivities

      assert(acts.size === 6)

      acts.foreach { act =>
        act.id.toString.take(4) match {
          case "act1" =>
            assert(act.dependsOn.size === 0)
          case "act2" =>
            assert(act.dependsOn.size === 0)
          case "act3" =>
            assert(act.dependsOn.map(_.id.toString.take(4)).toSet === Set("act1", "act2"))
            act.dependsOn.foreach { dependee => assert(dependee.dependsOn.size === 0) }
          case "act4" =>
            assert(act.dependsOn.map(_.id.toString.take(4)).toSet === Set("act2"))
            act.dependsOn.foreach { dependee => assert(dependee.dependsOn.size === 0) }
          case "act5" =>
            assert(act.dependsOn.map(_.id.toString.take(4)).toSet === Set("act3"))
            act.dependsOn.foreach { dependee =>
              assert(dependee.dependsOn.size === 2)
            }
          case "act6" =>
            assert(act.dependsOn.map(_.id.toString.take(4)).toSet === Set("act3", "act4"))
            act.dependsOn.foreach { dependee =>
              if (dependee.id.toString.take(4) == "act3") assert(dependee.dependsOn.size === 2)
              else assert(dependee.dependsOn.size === 1)
            }
          case _ => assert(true === false)  // should never happen
        }
      }
    }
  }
}
