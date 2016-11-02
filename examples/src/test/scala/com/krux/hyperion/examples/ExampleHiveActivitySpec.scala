package com.krux.hyperion.examples

import org.scalatest.WordSpec
import org.json4s.JsonDSL._
import org.json4s._

class ExampleHiveActivitySpec extends WordSpec {
  "ExampleHiveActivitySpec" should {
    "produce correct pipeline JSON" in {
      val pipelineJson = ExampleHiveActivity.toJson
      val objectsField = pipelineJson.children.head.children.sortBy(o => (o \ "name").toString)
      assert(objectsField.size == 5)

      val defaultObj = objectsField(1)
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

      val pipelineSchedule = objectsField(3)
      val pipelineScheduleShouldBe =
        ("id" -> "PipelineSchedule") ~
          ("name" -> "PipelineSchedule") ~
          ("period" -> "1 days") ~
          ("startAt" -> "FIRST_ACTIVATION_DATE_TIME") ~
          ("occurrences" -> "3") ~
          ("type" -> "Schedule")
      assert(pipelineSchedule === pipelineScheduleShouldBe)

      val dataNode = objectsField(4)
      val dataNodeId = (dataNode \ "id").values.toString
      assert(dataNodeId.startsWith("S3Folder_"))
      val dataNodeShouldBe =
        ("id" -> dataNodeId) ~
          ("name" -> dataNodeId) ~
          ("directoryPath" -> "#{my_S3Location}") ~
          ("type" -> "S3DataNode")
      assert(dataNode === dataNodeShouldBe)

      val hiveActivity = objectsField(2)
      val hiveActivityId = (hiveActivity \ "id").values.toString
      assert(hiveActivityId.startsWith("HiveActivity_"))
      val hiveActivityShouldBe =
        ("id" -> hiveActivityId) ~
          ("name" -> hiveActivityId) ~
          ("hiveScript" -> s"INSERT OVERWRITE TABLE $${output1} SELECT x.a FROM $${input1} x JOIN $${input2} y ON x.id = y.id;") ~
          ("stage" -> "true") ~
          ("input" -> Seq("ref" -> dataNodeId, "ref" -> dataNodeId)) ~
          ("output" -> Seq("ref" -> dataNodeId)) ~
          ("runsOn" -> ("ref" -> mapReduceClusterId)) ~
          ("type" -> "HiveActivity")
      assert(hiveActivity === hiveActivityShouldBe)
    }
  }
}
