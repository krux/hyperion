package com.krux.hyperion.examples

import org.scalatest.WordSpec
import org.json4s.JsonDSL._
import org.json4s._

class ExampleGoogleUploadActivitySpec extends WordSpec {

  "ExampleGoogleUploadActivitySpec" should {
    "produce correct pipeline JSON" in {
      val pipelineJson = ExampleGoogleUploadActivity.toJson
      val objectsField = pipelineJson.children.head.children.sortBy(o => (o \ "name").toString)
      assert(objectsField.size == 6)

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
          ("imageId" -> "ami-f6795a8c") ~
          ("instanceType" -> "m1.small") ~
          ("region" -> "us-east-1") ~
          ("securityGroups" -> Seq("your-security-group")) ~
          ("associatePublicIpAddress" -> "false") ~
          ("keyPair" -> "your-aws-key-pair") ~
          ("type" -> "Ec2Resource") ~
          ("role" -> "DataPipelineDefaultRole") ~
          ("resourceRole" -> "DataPipelineDefaultResourceRole") ~
          ("initTimeout" -> "1 hours")
      assert(ec2 === ec2ShouldBe)

      val pipelineSchedule = objectsField(4)
      val pipelineScheduleShouldBe =
        ("id" -> "PipelineSchedule") ~
          ("name" -> "PipelineSchedule") ~
          ("period" -> "1 days") ~
          ("startAt" -> "FIRST_ACTIVATION_DATE_TIME") ~
          ("type" -> "Schedule")
      assert(pipelineSchedule === pipelineScheduleShouldBe)

      val dataNode = objectsField(5)
      val dataNodeId = (dataNode \ "id").values.toString
      assert(dataNodeId.startsWith("S3File_"))
      val dataNodeShouldBe =
        ("id" -> dataNodeId) ~
          ("name" -> dataNodeId) ~
          ("filePath" -> "s3://the_source") ~
          ("type" -> "S3DataNode") ~
          ("s3EncryptionType" -> "SERVER_SIDE_ENCRYPTION")
      assert(dataNode === dataNodeShouldBe)

      val uploadActivity = objectsField(3)
      val uploadActivityId = (uploadActivity \ "id").values.toString
      assert(uploadActivityId.startsWith("GoogleStorageUploadActivity_"))
      val uploadActivityShouldBe =
        ("id" -> uploadActivityId) ~
          ("name" -> "Google Upload Activity") ~
          ("scriptUri" -> "s3://your-bucket/datapipeline/scripts/activities/gsutil-upload.sh") ~
          ("scriptArgument" -> Seq("s3://the config location", "gs://upload_location", "false")) ~
          ("stage" -> "true") ~
          ("input" -> Seq("ref" -> dataNodeId)) ~
          ("runsOn" -> ("ref" -> ec2Id)) ~
          ("type" -> "ShellCommandActivity")
      assert(uploadActivity === uploadActivityShouldBe)

      val recursiveUploadActivity = objectsField(2)
      val recursiveUploadActivityId = (recursiveUploadActivity \ "id").values.toString
      assert(recursiveUploadActivityId.startsWith("GoogleStorageUploadActivity_"))
      val recursiveUploadActivityShouldBe =
        ("id" -> recursiveUploadActivityId) ~
          ("name" -> "Google Upload Activity - Recursive") ~
          ("scriptUri" -> "s3://your-bucket/datapipeline/scripts/activities/gsutil-upload.sh") ~
          ("scriptArgument" -> Seq("s3://the config location", "gs://upload_location", "true")) ~
          ("stage" -> "true") ~
          ("input" -> Seq("ref" -> dataNodeId)) ~
          ("runsOn" -> ("ref" -> ec2Id)) ~
          ("dependsOn" -> List("ref" -> uploadActivityId)) ~
          ("type" -> "ShellCommandActivity")
      assert(recursiveUploadActivity === recursiveUploadActivityShouldBe)
    }
  }

}
