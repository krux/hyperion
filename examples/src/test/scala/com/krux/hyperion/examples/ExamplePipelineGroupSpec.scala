package com.krux.hyperion.examples

import org.scalatest.WordSpec
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.json4s._


class ExamplePipelineGroupSpec extends WordSpec {

  "ExamplePiplineGroup" should {

    val pipelines = ExamplePipelineGroup.ungroup()

    "Split into correct number of pipelines" in {

      assert(pipelines.size === 3)

      assert(pipelines.keys === Set(Some("1"), Some("2"), Some("3")))

      assert(
        pipelines.values.map(_.pipelineName).toSet === Set(
          "com.krux.hyperion.examples.ExamplePipelineGroup#1",
          "com.krux.hyperion.examples.ExamplePipelineGroup#2",
          "com.krux.hyperion.examples.ExamplePipelineGroup#3"
        )
      )

    }

    "Project the correct pipeline jsons" in pipelines.values.foreach { pipeline =>

      val pipelineJson: JValue = pipeline

      val objectsField = pipelineJson.children(0).children.sortBy(o => (o \ "name").toString)

      val default = objectsField(0)
      val defaultShouldBe =
        ("id" -> "Default") ~
        ("name" -> "Default") ~
        ("scheduleType" -> "cron") ~
        ("failureAndRerunMode" -> "CASCADE") ~
        ("pipelineLogUri" -> "s3://your-bucket/datapipeline-logs/") ~
        ("role" -> "DataPipelineDefaultRole") ~
        ("resourceRole" -> "DataPipelineDefaultResourceRole") ~
        ("schedule" -> ("ref" -> "PipelineSchedule"))
      assert(default === defaultShouldBe)

      val ec2 = objectsField(1)
      val ec2Id: String = (ec2 \ "id").values.toString
      assert(ec2Id.startsWith("Ec2Resource"))
      val ec2ShouldBe =
        ("id" -> ec2Id) ~
        ("name" -> ec2Id) ~
        ("terminateAfter" -> "8 hours") ~
        ("imageId" -> "ami-0188776c") ~
        ("instanceType" -> "m1.small") ~
        ("region" -> "us-east-1") ~
        ("securityGroups" -> Seq("your-security-group")) ~
        ("associatePublicIpAddress" -> "false") ~
        ("keyPair" -> "your-aws-key-pair") ~
        ("type" -> "Ec2Resource") ~
        ("role" -> "DataPipelineDefaultRole") ~
        ("resourceRole" -> "DataPipelineDefaultResourceRole")
      assert(ec2 === ec2ShouldBe)

      val pipelineSchedule = objectsField(2)
      val pipelineScheduleShouldBe =
        ("id" -> "PipelineSchedule") ~
        ("name" -> "PipelineSchedule") ~
        ("period" -> "1 days") ~
        ("occurrences" -> "1") ~
        ("startAt" -> "FIRST_ACTIVATION_DATE_TIME") ~
        ("type" -> "Schedule")
      assert(pipelineSchedule === pipelineScheduleShouldBe)

      pipeline.pipelineName match {
        case "com.krux.hyperion.examples.ExamplePipelineGroup#1" =>

          assert(objectsField.size === 8)

          val act0 = objectsField(3)
          val act0Id: String = (act0 \ "id").values.toString
          assert(act0Id.startsWith("ShellCommandActivity_"))
          val act0ShouldBe =
            ("id" -> act0Id) ~
            ("name" -> "act0") ~
            ("command" -> "echo 0") ~
            ("runsOn" -> ("ref" -> ec2Id)) ~
            ("type" -> "ShellCommandActivity")
          assert(act0 === act0ShouldBe)

          val act1 = objectsField(4)
          val act1Id: String = (act1 \ "id").values.toString
          assert(act1Id.startsWith("ShellCommandActivity_"))
          val act1ShouldBe =
            ("id" -> act1Id) ~
            ("name" -> "act1") ~
            ("command" -> "echo 1") ~
            ("runsOn" -> ("ref" -> ec2Id)) ~
            ("type" -> "ShellCommandActivity")
          assert(act1 === act1ShouldBe)

          val act2= objectsField(5)
          val act2Id: String = (act2 \ "id").values.toString
          assert(act2Id.startsWith("ShellCommandActivity_"))
          val act2ShouldBe =
            ("id" -> act2Id) ~
            ("name" -> "act2") ~
            ("command" -> "echo 2") ~
            ("runsOn" -> ("ref" -> ec2Id)) ~
            ("type" -> "ShellCommandActivity")
          assert(act2 === act2ShouldBe)

          val act3= objectsField(6)
          val act3Id: String = (act3 \ "id").values.toString
          assert(act3Id.startsWith("ShellCommandActivity_"))
          val act3ShouldBe =
            ("id" -> act3Id) ~
            ("name" -> "act3") ~
            ("command" -> "echo 3") ~
            ("runsOn" -> ("ref" -> ec2Id)) ~
            ("type" -> "ShellCommandActivity")
          assert(act3 === act3ShouldBe)

          val act4= objectsField(7)
          val act4Id: String = (act4 \ "id").values.toString
          assert(act4Id.startsWith("ShellCommandActivity_"))
          val act4ShouldBe =
            ("id" -> act4Id) ~
            ("name" -> "act4") ~
            ("command" -> "echo 4") ~
            ("runsOn" -> ("ref" -> ec2Id)) ~
            ("type" -> "ShellCommandActivity")
          assert(act4 === act4ShouldBe)

        case "com.krux.hyperion.examples.ExamplePipelineGroup#2" =>

          assert(objectsField.size === 4)

          val act5 = objectsField(3)
          val act5Id: String = (act5 \ "id").values.toString
          assert(act5Id.startsWith("ShellCommandActivity_"))
          val act5ShouldBe =
            ("id" -> act5Id) ~
            ("name" -> "act5") ~
            ("command" -> "echo 5") ~
            ("runsOn" -> ("ref" -> ec2Id)) ~
            ("type" -> "ShellCommandActivity")
          assert(act5 === act5ShouldBe)

        case "com.krux.hyperion.examples.ExamplePipelineGroup#3" =>

          assert(objectsField.size === 7)

          val act6 = objectsField(3)
          val act6Id: String = (act6 \ "id").values.toString
          assert(act6Id.startsWith("ShellCommandActivity_"))
          val act6ShouldBe =
            ("id" -> act6Id) ~
            ("name" -> "act6") ~
            ("command" -> "echo 6") ~
            ("runsOn" -> ("ref" -> ec2Id)) ~
            ("type" -> "ShellCommandActivity")
          assert(act6 === act6ShouldBe)

          val act7 = objectsField(4)
          val act7Id: String = (act7 \ "id").values.toString
          assert(act7Id.startsWith("ShellCommandActivity_"))
          val act7ShouldBe =
            ("id" -> act7Id) ~
            ("name" -> "act7") ~
            ("command" -> "echo 7") ~
            ("runsOn" -> ("ref" -> ec2Id)) ~
            ("type" -> "ShellCommandActivity")
          assert(act7 === act7ShouldBe)

          val act8 = objectsField(5)
          val act8Id: String = (act8 \ "id").values.toString
          assert(act8Id.startsWith("ShellCommandActivity_"))
          val act8ShouldBe =
            ("id" -> act8Id) ~
            ("name" -> "act8") ~
            ("command" -> "echo 8") ~
            ("runsOn" -> ("ref" -> ec2Id)) ~
            ("type" -> "ShellCommandActivity")
          assert(act8 === act8ShouldBe)

          val act9 = objectsField(6)
          val act9Id: String = (act9 \ "id").values.toString
          assert(act9Id.startsWith("ShellCommandActivity_"))
          val act9ShouldBe =
            ("id" -> act9Id) ~
            ("name" -> "act9") ~
            ("command" -> "echo 9") ~
            ("runsOn" -> ("ref" -> ec2Id)) ~
            ("type" -> "ShellCommandActivity")
          assert(act9 === act9ShouldBe)

        case _ =>
          assert(true === false)
      }

    }

  }

}
