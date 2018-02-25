package com.krux.hyperion.examples

import org.scalatest.WordSpec
import org.json4s.JsonDSL._
import org.json4s._

class ExampleMapReduceSpec extends WordSpec {

  "ExampleMapReduceSpec" should {

    "produce correct pipeline JSON" in {

      val pipelineJson = ExampleMapReduce.toJson
      val objectsField = pipelineJson.children.head.children.sortBy(o => (o \ "name").toString)

      // have the correct number of objects
      assert(objectsField.size === 7)

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

      val snsAlarm = objectsField(3)
      val snsAlarmId = (snsAlarm \ "id").values.toString
      assert(snsAlarmId.startsWith("SnsAlarm"))
      val snsAlarmShouldBe =
        ("id" -> snsAlarmId) ~
          ("name" -> snsAlarmId) ~
          ("subject" -> "Something happened at #{node.@scheduledStartTime}") ~
          ("message" -> "Some message #{my_InstanceCount} x #{my_InstanceType} @ #{my_InstanceBid} for #{my_S3Location}") ~
          ("topicArn" -> "arn:aws:sns:us-east-1:28619EXAMPLE:ExampleTopic") ~
          ("role" -> "DataPipelineDefaultResourceRole") ~
          ("type" -> "SnsAlarm")
      assert(snsAlarm === snsAlarmShouldBe)

      val mapReduceCluster = objectsField.head
      val mapReduceClusterId = (mapReduceCluster \ "id").values.toString
      assert(mapReduceClusterId.startsWith("EmrCluster_"))
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
        ("releaseLabel" -> "emr-4.4.0") ~
        ("initTimeout" -> "1 hours")
      assert(mapReduceCluster === mapReduceClusterShouldBe)

      val filterActivity = objectsField(4)
      val filterActivityId = (filterActivity \ "id").values.toString
      assert(filterActivityId.startsWith("EmrActivity_"))
      val filterActivityShouldBe =
        ("id" -> filterActivityId) ~
        ("name" -> "filterActivity") ~
        ("runsOn" -> ("ref" -> mapReduceClusterId)) ~
        ("step" -> List("s3://sample-jars/sample-jar-assembly-current.jar,com.krux.hyperion.ScoreJob1,the-target,#{format(minusDays(@scheduledStartTime,3),\"yyyy-MM-dd\")}")) ~
        ("onFail" -> List("ref" -> snsAlarmId)) ~
        ("type" -> "EmrActivity")
      assert(filterActivity === filterActivityShouldBe)

      val scoreActivity = objectsField(5)
      val scoreActivityId = (scoreActivity \ "id").values.toString
      assert(scoreActivityId.startsWith("EmrActivity_"))
      val scoreActivityShouldBe =
        ("id" -> scoreActivityId) ~
        ("name" -> "scoreActivity") ~
        ("runsOn" -> ("ref" -> mapReduceClusterId)) ~
        ("step" -> List("s3://sample-jars/sample-jar-assembly-current.jar,com.krux.hyperion.ScoreJob2,the-target,#{format(minusDays(@scheduledStartTime,3),\"yyyy-MM-dd\")},denormalized")) ~
        ("dependsOn" -> List("ref" -> filterActivityId)) ~
        ("onSuccess" -> List("ref" -> snsAlarmId)) ~
        ("type" -> "EmrActivity")
      assert(scoreActivity === scoreActivityShouldBe)

      val scoreHadoopActivity = objectsField(6)
      val scoreHadoopActivityId = (scoreHadoopActivity \ "id").values.toString
      assert(scoreHadoopActivityId.startsWith("HadoopActivity_"))
      val scoreHadoopActivityShouldBe =
        ("id" -> scoreHadoopActivityId) ~
        ("name" -> "scoreHadoopActivity") ~
        ("runsOn" -> ("ref" -> mapReduceClusterId)) ~
        ("jarUri" -> "s3://sample-jars/sample-jar-assembly-current.jar") ~
        ("mainClass"-> "com.krux.hyperion.ScoreJob2") ~
        ("argument" -> List("the-target", "#{format(minusDays(@scheduledStartTime,3),\"yyyy-MM-dd\")}", "denormalized")) ~
        ("dependsOn" -> List("ref" -> scoreActivityId)) ~
        ("onSuccess" -> List("ref" -> snsAlarmId)) ~
        ("type" -> "HadoopActivity")
      assert(scoreHadoopActivity === scoreHadoopActivityShouldBe)

    }
  }

}

