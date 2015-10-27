package com.krux.hyperion.examples

import com.krux.hyperion.DataPipelineDef._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.scalatest.WordSpec

class ExampleWorkflowSpec extends WordSpec {

  "ExampleWorkflow" should {

    "produce correct pipeline JSON" in {
      val pipelineJson: JValue = ExampleWorkflow
      val objectsField = (pipelineJson \ "objects").children.sortBy(o => (o \ "id").toString)

      assert(objectsField.size === 9)  // 6 activities, 1 default, 1 schedule, 1 ec2 resource

      // the first object should be Default
      val defaultObj = objectsField.head
      val defaultObjShouldBe = ("id" -> "Default") ~
        ("name" -> "Default") ~
        ("scheduleType" -> "cron") ~
        ("failureAndRerunMode" -> "CASCADE") ~
        ("pipelineLogUri" -> "s3://your-bucket/datapipeline-logs/") ~
        ("role" -> "DataPipelineDefaultRole") ~
        ("resourceRole" -> "DataPipelineDefaultResourceRole") ~
        ("schedule" -> ("ref" -> "PipelineSchedule"))
      assert(defaultObj === defaultObjShouldBe)

      val ec2 = objectsField(1)
      val ec2Id: String = (ec2 \ "id").values.toString
      assert(ec2Id.startsWith("Ec2Resource"))
      val ec2ShouldBe =
        ("id" -> ec2Id) ~
        ("name" -> ec2Id) ~
        ("terminateAfter" -> "8 hours") ~
        ("imageId" -> "ami-b0682cd8") ~
        ("instanceType" -> "m1.small") ~
        ("region" -> "us-east-1") ~
        ("securityGroups" -> Seq("your-security-group")) ~
        ("associatePublicIpAddress" -> "false") ~
        ("keyPair" -> "your-aws-key-pair") ~
        ("spotBidPrice" -> "#{my_SpotPrice}") ~
        ("type" -> "Ec2Resource") ~
        ("role" -> "DataPipelineDefaultRole") ~
        ("resourceRole" -> "DataPipelineDefaultResourceRole")

      assert(ec2 === ec2ShouldBe)

      val pipelineSchedule = objectsField(2)
      val pipelineScheduleShouldBe =
        ("id" -> "PipelineSchedule") ~
        ("name" -> "PipelineSchedule") ~
        ("period" -> "1 days") ~
        ("startAt" -> "FIRST_ACTIVATION_DATE_TIME") ~
        ("type" -> "Schedule")
      assert(pipelineSchedule === pipelineScheduleShouldBe)

      val act1 = objectsField(3)
      val act1Id = (act1 \ "id").values.toString
      assert(act1Id.startsWith("act1"))
      val act1ShouldBe =
        ("id" -> act1Id) ~
        ("name" -> act1Id) ~
        ("command" -> "run act1") ~
        ("runsOn" -> ("ref" -> ec2Id)) ~
        ("type" -> "ShellCommandActivity")
      assert(act1ShouldBe === act1)

      val act2 = objectsField(4)
      val act2Id = (act2 \ "id").values.toString
      assert(act2Id.startsWith("act2"))
      val act2ShouldBe =
        ("id" -> act2Id) ~
        ("name" -> act2Id) ~
        ("command" -> "run act2") ~
        ("runsOn" -> ("ref" -> ec2Id)) ~
        ("dependsOn" -> List("ref" -> act1Id)) ~
        ("type" -> "ShellCommandActivity")
      assert(act2ShouldBe === act2)

      val act3 = objectsField(5)
      val act3Id = (act3 \ "id").values.toString
      assert(act3Id.startsWith("act3"))
      val act3ShouldBe =
        ("id" -> act3Id) ~
        ("name" -> act3Id) ~
        ("command" -> "run act3") ~
        ("runsOn" -> ("ref" -> ec2Id)) ~
        ("dependsOn" -> List("ref" -> act1Id)) ~
        ("type" -> "ShellCommandActivity")
      assert(act3ShouldBe === act3)

      val act4 = objectsField(6)
      val act4Id = (act4 \ "id").values.toString
      assert(act4Id.startsWith("act4"))
      val act4ShouldBe =
        ("id" -> act4Id) ~
        ("name" -> act4Id) ~
        ("command" -> "run act4") ~
        ("runsOn" -> ("ref" -> ec2Id)) ~
        ("dependsOn" -> List("ref" -> act2Id, "ref" -> act3Id)) ~
        ("type" -> "ShellCommandActivity")
      assert(act4ShouldBe === act4)

      val act5 = objectsField(7)
      val act5Id = (act5 \ "id").values.toString
      assert(act5Id.startsWith("act5"))
      val act5ShouldBe =
        ("id" -> act5Id) ~
        ("name" -> act5Id) ~
        ("command" -> "run act5") ~
        ("runsOn" -> ("ref" -> ec2Id)) ~
        ("dependsOn" -> List("ref" -> act2Id, "ref" -> act3Id)) ~
        ("type" -> "ShellCommandActivity")
      assert(act5ShouldBe === act5)

      val act6 = objectsField(8)
      val act6Id = (act6 \ "id").values.toString
      assert(act6Id.startsWith("act6"))
      val act6ShouldBe =
        ("id" -> act6Id) ~
        ("name" -> act6Id) ~
        ("command" -> "run act6") ~
        ("runsOn" -> ("ref" -> ec2Id)) ~
        ("dependsOn" -> List("ref" -> act4Id, "ref" -> act5Id)) ~
        ("type" -> "ShellCommandActivity")
      assert(act6ShouldBe === act6)

    }
  }

}
