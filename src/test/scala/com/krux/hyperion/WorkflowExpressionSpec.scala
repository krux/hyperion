package com.krux.hyperion

import com.krux.hyperion.activity.ShellCommandActivity
import com.krux.hyperion.resource.Ec2Resource
import com.krux.hyperion.WorkflowDSL._
import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec

class WorkflowExpressionSpec extends WordSpec {

  "WorkflowExpression" should {

    implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

    val ec2 = Ec2Resource()

    "produce correct dependencies with no duplicates" in {

      val act1 = ShellCommandActivity(ec2).withCommand("run act1").named("act1")
      val act2 = ShellCommandActivity(ec2).withCommand("run act2").named("act2")
      val act3 = ShellCommandActivity(ec2).withCommand("run act3").named("act3")
      val act4 = ShellCommandActivity(ec2).withCommand("run act4").named("act4")
      val act5 = ShellCommandActivity(ec2).withCommand("run act5").named("act5")
      val act6 = ShellCommandActivity(ec2).withCommand("run act6").named("act6")

      val dependencies = (act1 + act2) :~> ((act3 :~> act4) + act5) :~> act6

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

  }

}