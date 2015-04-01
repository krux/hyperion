package com.krux.hyperion.examples

import org.scalatest.WordSpec
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.json4s._
import com.krux.hyperion.DataPipelineDef._

class ExampleSparkSpec extends WordSpec {

  "ExampleSparkSpec" should {

    "produce correct pipeline JSON" in {

      val pipelineJson: JValue = ExampleSpark
      val objectsField = pipelineJson.children(0).children

      // have the correct number of objects
      assert(objectsField.size === 6)

      // the first object should be Default
      val defaultObj = objectsField(0)
      val defaultObjShouldBe = ("id" -> "Default") ~
        ("name" -> "Default") ~
        ("scheduleType" -> "cron") ~
        ("failureAndRerunMode" -> "CASCADE") ~
        ("pipelineLogUri" -> "s3://your-bucket/datapipeline-logs/") ~
        ("role" -> "DataPipelineDefaultRole") ~
        ("resourceRole" -> "DataPipelineDefaultResourceRole") ~
        ("schedule" -> ("ref" -> "PipelineSchedule"))
      assert(defaultObj === defaultObjShouldBe)

      val pipelineSchedule = objectsField(1)
      val pipelineScheduleShouldBe =
        ("id" -> "PipelineSchedule") ~
        ("name" -> "PipelineSchedule") ~
        ("period" -> "1 days") ~
        ("startAt" -> "FIRST_ACTIVATION_DATE_TIME") ~
        ("occurrences" -> "3") ~
        ("type" -> "Schedule")
      assert(pipelineSchedule === pipelineScheduleShouldBe)

      val sparkCluster = objectsField(2)
      val sparkClusterShouldBe =
        ("id" -> "SparkCluster") ~
        ("name" -> "SparkCluster") ~
        ("bootstrapAction" -> List("s3://support.elasticmapreduce/spark/install-spark,-v,1.1.1.e,-x", "s3://your-bucket/datapipeline/scripts/deploy-hyperion-emr-env.sh,s3://bucket/org_env.sh")) ~
        ("amiVersion" -> "3.3") ~
        ("masterInstanceType" -> "m3.xlarge") ~
        ("coreInstanceType" -> "m3.xlarge") ~
        ("coreInstanceCount" -> "2") ~
        ("taskInstanceType" -> "m3.xlarge") ~
        ("taskInstanceCount" -> "1") ~
        ("terminateAfter" -> "8 hours") ~
        ("keyPair" -> "your-aws-key-pair") ~
        ("type" -> "EmrCluster")
      assert(sparkCluster === sparkClusterShouldBe)

      val snsAlarm = objectsField(3)
      val snsAlarmShouldBe =
        ("id" -> "sns-alarm-1") ~
        ("name" -> "sns-alarm-1") ~
        ("subject" -> "Something happened at #{node.@scheduledStartTime}") ~
        ("message" -> "Some message #{myInstanceCount} x #{myInstanceType} @ #{myInstanceBid} for #{myS3Location}") ~
        ("topicArn" -> "arn:aws:sns:us-east-1:28619EXAMPLE:ExampleTopic") ~
        ("role" -> "DataPipelineDefaultResourceRole") ~
        ("type" -> "SnsAlarm")
      assert(snsAlarm === snsAlarmShouldBe)

      val scoreActivity = objectsField(4)
      val scoreActivityShouldBe =
        ("id" -> "scoreActivity") ~
        ("name" -> "scoreActivity") ~
        ("runsOn" -> ("ref" -> "SparkCluster")) ~
        ("step" -> List(
          "s3://elasticmapreduce/libs/script-runner/script-runner.jar,s3://your-bucket/datapipeline/scripts/run-spark-step.sh,s3://sample-jars/sample-jar-assembly-current.jar,com.krux.hyperion.ScoreJob1,the-target,#{format(minusDays(@scheduledStartTime,3),'yyyy-MM-dd')},denormalized",
          "s3://elasticmapreduce/libs/script-runner/script-runner.jar,s3://your-bucket/datapipeline/scripts/run-spark-step.sh,s3://sample-jars/sample-jar-assembly-current.jar,com.krux.hyperion.ScoreJob2,the-target,#{format(minusDays(@scheduledStartTime,3),'yyyy-MM-dd')}"
          )) ~
        ("dependsOn" -> List("ref" -> "filterActivity")) ~
        ("onSuccess" -> List("ref" -> "sns-alarm-1")) ~
        ("type" -> "EmrActivity")
      assert(scoreActivity === scoreActivityShouldBe)

      val filterActivity = objectsField(5)
      val filterActivityShouldBe =
        ("id" -> "filterActivity") ~
        ("name" -> "filterActivity") ~
        ("runsOn" -> ("ref" -> "SparkCluster")) ~
        ("step" ->
          List("s3://elasticmapreduce/libs/script-runner/script-runner.jar,s3://your-bucket/datapipeline/scripts/run-spark-step.sh,s3://sample-jars/sample-jar-assembly-current.jar,com.krux.hyperion.FilterJob,the-target,#{format(minusDays(@scheduledStartTime,3),'yyyy-MM-dd')}")) ~
        ("onFail" -> List("ref" -> "sns-alarm-1")) ~
        ("type" -> "EmrActivity")
      assert(filterActivity === filterActivityShouldBe)

    }
  }

}
