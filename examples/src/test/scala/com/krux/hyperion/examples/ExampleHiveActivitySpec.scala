package com.krux.hyperion.examples

import org.scalatest.WordSpec
import org.json4s.JsonDSL._
import org.json4s._

class ExampleHiveActivitySpec extends WordSpec {
  "ExampleHiveActivitySpec" should {
    "produce correct pipeline JSON" in {
      val pipelineJson = ExampleHiveActivity.toJson
      val objectsField = pipelineJson.children.head.children.sortBy(o => (o \ "name").toString)
      assert(objectsField.size == 8)

      val dataFormat = objectsField(1)
      val dataFormatId = (dataFormat \ "id").values.toString
      assert(dataFormatId.startsWith("CsvDataFormat_"))
      val dataFormatShouldBe =
        ("id" -> dataFormatId) ~
          ("name" -> dataFormatId) ~
          ("column" -> List("id STRING", "a STRING")) ~
          ("columnSeparator" -> "^") ~
          ("recordSeparator" -> "\n") ~
          ("type" -> "Custom")
      assert(dataFormat === dataFormatShouldBe)

      val defaultObj = objectsField(2)
      val defaultObjShouldBe = ("id" -> "Default") ~
        ("name" -> "Default") ~
        ("scheduleType" -> "cron") ~
        ("failureAndRerunMode" -> "CASCADE") ~
        ("pipelineLogUri" -> "s3://your-bucket/datapipeline-logs/") ~
        ("role" -> "DataPipelineDefaultRole") ~
        ("resourceRole" -> "DataPipelineDefaultResourceRole") ~
        ("schedule" -> ("ref" -> "PipelineSchedule"))
      assert(defaultObj === defaultObjShouldBe)

      val mapReduceCluster = objectsField.head
      val mapReduceClusterId = (mapReduceCluster \ "id").values.toString
      assert(mapReduceClusterId.startsWith("MapReduceCluster_"))
      val mapReduceClusterShouldBe =
        ("id" -> mapReduceClusterId) ~
          ("name" -> "Cluster with release label") ~
          ("bootstrapAction" -> Seq.empty[String]) ~
          ("masterInstanceType" -> "m3.xlarge") ~
          ("coreInstanceType" -> "m3.xlarge") ~
          ("coreInstanceCount" -> "2") ~
          ("taskInstanceType" -> "#{my_InstanceType}") ~
          ("taskInstanceCount" -> "#{my_InstanceCount}") ~
          ("terminateAfter" -> "8 hours") ~
          ("keyPair" -> "your-aws-key-pair") ~
          ("type" -> "EmrCluster") ~
          ("region" -> "us-east-1") ~
          ("role" -> "DataPipelineDefaultRole") ~
          ("resourceRole" -> "DataPipelineDefaultResourceRole") ~
          ("releaseLabel" -> "emr-4.4.0")
      assert(mapReduceCluster === mapReduceClusterShouldBe)

      val pipelineSchedule = objectsField(4)
      val pipelineScheduleShouldBe =
        ("id" -> "PipelineSchedule") ~
          ("name" -> "PipelineSchedule") ~
          ("period" -> "1 days") ~
          ("startAt" -> "FIRST_ACTIVATION_DATE_TIME") ~
          ("occurrences" -> "3") ~
          ("type" -> "Schedule")
      assert(pipelineSchedule === pipelineScheduleShouldBe)

      val input1 = objectsField(5)
      val input1Id = (input1 \ "id").values.toString
      assert(input1Id.startsWith("S3Folder_"))
      val input1ShouldBe =
        ("id" -> input1Id) ~
          ("name" -> input1Id) ~
          ("dataFormat" -> ("ref" -> dataFormatId)) ~
          ("directoryPath" -> "s3://source/input1") ~
          ("type" -> "S3DataNode")
      assert(input1 === input1ShouldBe)

      val input2 = objectsField(6)
      val input2Id = (input2 \ "id").values.toString
      assert(input2Id.startsWith("S3Folder_"))
      val input2ShouldBe =
        ("id" -> input2Id) ~
          ("name" -> input2Id) ~
          ("dataFormat" -> ("ref" -> dataFormatId)) ~
          ("directoryPath" -> "s3://source/input2") ~
          ("type" -> "S3DataNode")
      assert(input2 === input2ShouldBe)

      val output = objectsField(7)
      val outputId = (output \ "id").values.toString
      assert(outputId.startsWith("S3Folder_"))
      val outputShouldBe =
        ("id" -> outputId) ~
          ("name" -> outputId) ~
          ("dataFormat" -> ("ref" -> dataFormatId)) ~
          ("directoryPath" -> "s3://dest") ~
          ("type" -> "S3DataNode")
      assert(output === outputShouldBe)

      val hiveActivity = objectsField(3)
      val hiveActivityId = (hiveActivity \ "id").values.toString
      assert(hiveActivityId.startsWith("HiveActivity_"))
      val hiveActivityShouldBe =
        ("id" -> hiveActivityId) ~
          ("name" -> hiveActivityId) ~
          ("hiveScript" -> s"INSERT OVERWRITE TABLE $${output1} SELECT x.a FROM $${input1} x JOIN $${input2} y ON x.id = y.id;") ~
          ("stage" -> "true") ~
          ("input" -> Seq("ref" -> input1Id, "ref" -> input2Id)) ~
          ("output" -> Seq("ref" -> outputId)) ~
          ("runsOn" -> ("ref" -> mapReduceClusterId)) ~
          ("type" -> "HiveActivity")
      assert(hiveActivity === hiveActivityShouldBe)
    }
  }
}
