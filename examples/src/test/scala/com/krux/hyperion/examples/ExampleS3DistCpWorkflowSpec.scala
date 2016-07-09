package com.krux.hyperion.examples

import org.scalatest.WordSpec
import org.json4s.JsonDSL._
import org.json4s._
import com.krux.hyperion.DataPipelineDef._

class ExampleS3DistCpWorkflowSpec extends WordSpec {

  "ExampleS3DistCpWorkflowSpec" should {

    "produce correct pipeline JSON" in {

      val pipelineJson: JValue = ExampleS3DistCpWorkflow
      val objectsField = pipelineJson.children.head.children.sortBy(o => (o \ "name").toString)

      // have the correct number of objects
      assert(objectsField.size === 4)

      // the first object should be Default
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

      val pipelineSchedule = objectsField(2)
      val pipelineScheduleShouldBe =
        ("id" -> "PipelineSchedule") ~
        ("name" -> "PipelineSchedule") ~
        ("period" -> "1 days") ~
        ("startAt" -> "FIRST_ACTIVATION_DATE_TIME") ~
        ("occurrences" -> "3") ~
        ("type" -> "Schedule")
      assert(pipelineSchedule === pipelineScheduleShouldBe)

      val mapReduceCluster = objectsField(0)
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

      val s3DistCpActivity = objectsField(3)
      val s3DistCpActivityyId = (s3DistCpActivity \ "id").values.toString
      assert(s3DistCpActivityyId.startsWith("S3DistCpActivity_"))
      val filterActivityShouldBe =
        ("id" -> s3DistCpActivityyId) ~
        ("name" -> "s3DistCpActivity") ~
        ("runsOn" -> ("ref" -> mapReduceClusterId)) ~
        ("step" -> List("command-runner.jar,s3-dist-cp,--src,s3://the-source,--dest,hdfs:///the-target,--outputCodec,gz")) ~
        ("type" -> "EmrActivity")
      assert(s3DistCpActivity === filterActivityShouldBe)

    }
  }
}
