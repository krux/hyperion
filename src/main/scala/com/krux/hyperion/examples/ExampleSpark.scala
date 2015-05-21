package com.krux.hyperion.examples

import com.krux.hyperion.{Schedule, DataPipelineDef, HyperionContext}
import com.krux.hyperion.Implicits._
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.activity.{SparkActivity, SparkStep}
import com.krux.hyperion.expression.DateTimeFunctions.format
import com.krux.hyperion.expression.ExpressionDSL._
import com.krux.hyperion.parameter._
import com.krux.hyperion.resource.SparkCluster
import com.typesafe.config.ConfigFactory

object ExampleSpark extends DataPipelineDef {

  val target = "the-target"
  val jar = "s3://sample-jars/sample-jar-assembly-current.jar"

  override implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

  override lazy val tags = Map("example" -> None, "ownerGroup" -> Some("spark"))

  override lazy val schedule = Schedule()
    .startAtActivation
    .every(1.day)
    .stopAfter(3)

  val location = S3KeyParameter("S3Location", "s3://your-location/")
  val instanceType = StringParameter("InstanceType", "c3.8xlarge")
  val instanceCount = IntegerParameter("InstanceCount", 8)
  val instanceBid = DoubleParameter("InstanceBid", 3.40)

  override def parameters: Iterable[Parameter] = Seq(location, instanceType, instanceCount, instanceBid)

  override def workflow = {

    // Actions
    val mailAction = SnsAlarm()
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
      .withArguments(
        target,
        format(SparkActivity.scheduledStartTime - 3.days, "yyyy-MM-dd")
      )

    val filterActivity = SparkActivity(sparkCluster)
      .named("filterActivity")
      .withSteps(filterStep)
      .onFail(mailAction)

    // Second activity
    val scoreStep1 = SparkStep()
      .withJar(jar)
      .withMainClass("com.krux.hyperion.ScoreJob1")
      .withArguments(
        target,
        format(SparkActivity.scheduledStartTime - 3.days, "yyyy-MM-dd"),
        "denormalized"
      )

    val scoreStep2 = SparkStep()
      .withJar(jar)
      .withMainClass("com.krux.hyperion.ScoreJob2")
      .withArguments(target, format(SparkActivity.scheduledStartTime - 3.days, "yyyy-MM-dd"))

    val scoreActivity = SparkActivity(sparkCluster)
      .named("scoreActivity")
      .withSteps(scoreStep1, scoreStep2)
      .dependsOn(filterActivity)
      .onSuccess(mailAction)

    Seq(scoreActivity)

  }

}
