package com.krux.hyperion.examples

import com.krux.hyperion.DataPipelineDef
import com.krux.hyperion.Implicits._
import com.krux.hyperion.objects.{Schedule, SparkCluster, SparkStep, SnsAlarm, S3KeyParameter,
  SparkActivity, PipelineObject, Parameter, StringParameter, IntegerParameter, DoubleParameter}
import com.krux.hyperion.expressions.DateTimeFunctions.format
import com.krux.hyperion.expressions.ExpressionDSL._
import com.krux.hyperion.HyperionContext
import com.typesafe.config.ConfigFactory

object ExampleSpark extends DataPipelineDef {

  val target = "the-target"
  val jar = "s3://sample-jars/sample-jar-assembly-current.jar"

  override implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

  override lazy val schedule = Schedule()
    .startAtActivation
    .every(1.day)
    .stopAfter(3)

  val location = S3KeyParameter("S3Location", "s3://krux-temp/")
  val instanceType = StringParameter("InstanceType", "c3.8xlarge")
  val instanceCount = IntegerParameter("InstanceCount", 8)
  val instanceBid = DoubleParameter("InstanceBid", 3.40)

  override def parameters: Iterable[Parameter] = Seq(location, instanceType, instanceCount, instanceBid)

  override def workflow = {

    // Actions
    val mailAction = SnsAlarm("sns-alarm-1")
      .withSubject("Something happened at #{node.@scheduledStartTime}")
      .withMessage(s"Some message ${instanceCount} x ${instanceType} @ ${instanceBid} for ${location}")
      .withTopicArn("arn:aws:sns:us-east-1:28619EXAMPLE:ExampleTopic")
      .withRole("DataPipelineDefaultResourceRole")

    // Resources
    val sparkCluster = SparkCluster().withTaskInstanceCount(1)

    // First activity
    val filterStep = SparkStep()
      .withJar(jar)
      .withMainClass("com.krux.hyperion.FilterJob")
      .withArgs(
        target,
        format(SparkActivity.scheduledStartTime - 3.days, "yyyy-MM-dd")
      )

    val filterActivity = SparkActivity("filterActivity", sparkCluster)
      .withSteps(filterStep)
      .onFail(mailAction)

    // Second activity
    val scoreStep1 = SparkStep()
      .withJar(jar)
      .withMainClass("com.krux.hyperion.ScoreJob1")
      .withArgs(
        target,
        format(SparkActivity.scheduledStartTime - 3.days, "yyyy-MM-dd"),
        "denormalized"
      )

    val scoreStep2 = SparkStep()
      .withJar(jar)
      .withMainClass("com.krux.hyperion.ScoreJob2")
      .withArgs(target, format(SparkActivity.scheduledStartTime - 3.days, "yyyy-MM-dd"))

    val scoreActivity = SparkActivity("scoreActivity", sparkCluster)
      .withSteps(scoreStep1, scoreStep2)
      .dependsOn(filterActivity)
      .onSuccess(mailAction)

    Seq(scoreActivity)

  }

}
