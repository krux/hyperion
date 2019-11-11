package com.krux.hyperion.workflow

import org.scalatest.{ Matchers, WordSpec }

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.activity.ShellCommandActivity
import com.krux.hyperion.resource.Ec2Resource
import com.krux.hyperion.workflow.WorkflowExpression._
import com.typesafe.config.ConfigFactory

class WorkflowExpressionSpec extends WordSpec with Matchers {

  "WorkflowExpression" should {

    implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

    val ec2 = Ec2Resource()

    "produce correct dependencies with no duplicates" in {

      val act1 = ShellCommandActivity("run act1")(ec2).idNamed("act1")
      val act2 = ShellCommandActivity("run act2")(ec2).idNamed("act2")
      val act3 = ShellCommandActivity("run act3")(ec2).idNamed("act3")
      val act4 = ShellCommandActivity("run act4")(ec2).idNamed("act4")
      val act5 = ShellCommandActivity("run act5")(ec2).idNamed("act5")
      val act6 = ShellCommandActivity("run act6")(ec2).idNamed("act6")

      val dependencies = (act1 + act2) ~> ((act3 ~> act4) + act5) ~> act6

      val rAct3 = act3.dependsOn(act1, act2)
      val rAct4 = act4.dependsOn(rAct3)
      val rAct5 = act5.dependsOn(act1, act2)
      val rAct6 = act6.dependsOn(rAct4, rAct5)

      dependencies.toActivities should contain theSameElementsAs Seq(
        act1, act2, rAct3, rAct4, rAct5, rAct6
      )
    }

    "produce correct dependencies for straight arrow" in {
      val act1 = ShellCommandActivity("run act1")(ec2).idNamed("act1")
      val act2 = ShellCommandActivity("run act2")(ec2).idNamed("act2")
      val act3 = ShellCommandActivity("run act3")(ec2).idNamed("act3")
      val act4 = ShellCommandActivity("run act4")(ec2).idNamed("act4")

      val dependencies = act1 ~> (act2 ~> act3) ~> act4

      val rAct2 = act2.dependsOn(act1)
      val rAct3 = act3.dependsOn(rAct2)
      val rAct4 = act4.dependsOn(rAct3)

      dependencies.toActivities should contain theSameElementsAs Seq(
        act1, rAct2, rAct3, rAct4
      )
    }

    "produce correct dependencies with duplicates" in {

      val act1 = ShellCommandActivity("run act1")(ec2).idNamed("act1")
      val act2 = ShellCommandActivity("run act2")(ec2).idNamed("act2")
      val act3 = ShellCommandActivity("run act3")(ec2).idNamed("act3")
      val act4 = ShellCommandActivity("run act4")(ec2).idNamed("act4")
      val act5 = ShellCommandActivity("run act5")(ec2).idNamed("act5")
      val act6 = ShellCommandActivity("run act6")(ec2).idNamed("act6")

      // equivalent to val dependencies = (act1 + act2) ~> ((act3 ~> act4) + act5) ~> act6
      val dependencies =
        (act1 ~> act3) +
        (act2 ~> act3) +
        (act3 ~> act4) +
        (act2 ~> act5) +
        (act1 ~> act5) +
        (act4 ~> act6) +
        (act5 ~> act6)


      val rAct3 = act3.dependsOn(act1, act2)
      val rAct4 = act4.dependsOn(rAct3)
      val rAct5 = act5.dependsOn(act1, act2)
      val rAct6 = act6.dependsOn(rAct4, rAct5)

      dependencies.toActivities should contain theSameElementsAs Seq(
        act1, act2, rAct3, rAct4, rAct5, rAct6
      )
    }

    "detect inconsistent duplicated ids" in {
      val act1 = ShellCommandActivity("run act1")(ec2).idNamed("act1")
      val act2 = ShellCommandActivity("run act2")(ec2).idNamed("act2")
      val act3 = ShellCommandActivity("run act3")(ec2).idNamed("act3")

      val dependencies =
        (act1 ~> act3) +
        (act2 ~> act3.withArguments("modified"))

      an [AssertionError] should be thrownBy dependencies.toActivities
    }

    "detect circular dependencies" in {
      val act1 = ShellCommandActivity("run act1")(ec2).idNamed("act1")
      val act2 = ShellCommandActivity("run act2")(ec2).idNamed("act2")

      val dependencies =
        (act1 ~> act2) +
        (act2 ~> act1)

      an [AssertionError] should be thrownBy dependencies.toActivities
    }
  }


}
