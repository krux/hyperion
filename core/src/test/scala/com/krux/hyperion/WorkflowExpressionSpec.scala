package com.krux.hyperion

import com.krux.hyperion.activity.ShellCommandActivity
import com.krux.hyperion.resource.Ec2Resource
import com.krux.hyperion.WorkflowExpression._
import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec

class WorkflowExpressionSpec extends WordSpec {

  "WorkflowExpression" should {

    implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

    val ec2 = Ec2Resource()

    "produce correct dependencies with no duplicates" in {

      val act1 = ShellCommandActivity("run act1")(ec2).named("act1")
      val act2 = ShellCommandActivity("run act2")(ec2).named("act2")
      val act3 = ShellCommandActivity("run act3")(ec2).named("act3")
      val act4 = ShellCommandActivity("run act4")(ec2).named("act4")
      val act5 = ShellCommandActivity("run act5")(ec2).named("act5")
      val act6 = ShellCommandActivity("run act6")(ec2).named("act6")

      val dependencies = (act1 + act2) ~> ((act3 ~> act4) + act5) ~> act6

      val activities = dependencies.toPipelineObjects

      activities.foreach { act =>
        act.id.toString.take(4) match {
          case "act1" =>
            assert(act.dependsOn.size === 0)
          case "act2" =>
            assert(act.dependsOn.size === 0)
          case "act3" =>
            assert(act.dependsOn.size === 2)
            val dependeeIds = act.dependsOn.map(_.id.toString.take(4)).toSet
            assert(dependeeIds === Set("act1", "act2"))
          case "act4" =>
            assert(act.dependsOn.size === 3)
            val dependeeIds = act.dependsOn.map(_.id.toString.take(4)).toSet
            assert(dependeeIds === Set("act1", "act2", "act3"))
          case "act5" =>
            assert(act.dependsOn.size === 2)
            val dependeeIds = act.dependsOn.map(_.id.toString.take(4)).toSet
            assert(dependeeIds === Set("act1", "act2"))
          case "act6" =>
            assert(act.dependsOn.size === 5)
            val dependeeIds = act.dependsOn.map(_.id.toString.take(4)).toSet
            assert(dependeeIds === Set("act1", "act2", "act3", "act4", "act5"))
          case _ =>
            // this should never get executed
            assert(true === false)
        }
      }

    }

    "produce correct dependencies for straight arrow" in {
      val act1 = ShellCommandActivity("run act1")(ec2).named("act1")
      val act2 = ShellCommandActivity("run act2")(ec2).named("act2")
      val act3 = ShellCommandActivity("run act3")(ec2).named("act3")
      val act4 = ShellCommandActivity("run act4")(ec2).named("act4")

      val dependencies = act1 ~> act2 ~> act3 ~> act4
      val activities = dependencies.toPipelineObjects

      activities.foreach { act =>
        act.id.toString.take(4) match {
          case "act1" =>
            assert(act.dependsOn.size === 0)
          case "act2" =>
            assert(act.dependsOn.size === 1)
            val dependeeIds = act.dependsOn.map(_.id.toString.take(4)).toSet
            assert(dependeeIds === Set("act1"))
          case "act3" =>
            assert(act.dependsOn.size === 2)
            val dependeeIds = act.dependsOn.map(_.id.toString.take(4)).toSet
            assert(dependeeIds === Set("act1", "act2"))
          case "act4" =>
            assert(act.dependsOn.size === 3)
            val dependeeIds = act.dependsOn.map(_.id.toString.take(4)).toSet
            assert(dependeeIds === Set("act1", "act2", "act3"))
          case _ =>
            // this should never get executed
            assert(true === false)
        }
      }
    }

    "produce correct dependencies with duplicates" in {

      val act1 = ShellCommandActivity("run act1")(ec2).named("act1")
      val act2 = ShellCommandActivity("run act2")(ec2).named("act2")
      val act3 = ShellCommandActivity("run act3")(ec2).named("act3")
      val act4 = ShellCommandActivity("run act4")(ec2).named("act4")
      val act5 = ShellCommandActivity("run act5")(ec2).named("act5")
      val act6 = ShellCommandActivity("run act6")(ec2).named("act6")

      // equivalent to val dependencies = (act1 + act2) ~> ((act3 ~> act4) + act5) ~> act6
      val dependencies =
        (act1 ~> act3) +
        (act2 ~> act3) +
        (act3 ~> act4) +
        (act2 ~> act5) +
        (act1 ~> act5) +
        (act4 ~> act6) +
        (act5 ~> act6)

      val activities = dependencies.toPipelineObjects

      activities.foreach { act =>
        act.id.toString.take(4) match {
          case "act1" =>
            assert(act.dependsOn.size === 0)
          case "act2" =>
            assert(act.dependsOn.size === 0)
          case "act3" =>
            assert(act.dependsOn.size === 2)
            val dependeeIds = act.dependsOn.map(_.id.toString.take(4)).toSet
            assert(dependeeIds === Set("act1", "act2"))
          case "act4" =>
            assert(act.dependsOn.size === 1)
            val dependeeIds = act.dependsOn.map(_.id.toString.take(4)).toSet
            assert(dependeeIds === Set("act3"))
          case "act5" =>
            assert(act.dependsOn.size === 2)
            val dependeeIds = act.dependsOn.map(_.id.toString.take(4)).toSet
            assert(dependeeIds === Set("act1", "act2"))
          case "act6" =>
            assert(act.dependsOn.size === 2)
            val dependeeIds = act.dependsOn.map(_.id.toString.take(4)).toSet
            assert(dependeeIds === Set("act5", "act4"))
          case _ =>
            // this should never get executed
            assert(true === false)
        }
      }

    }

  }


}
