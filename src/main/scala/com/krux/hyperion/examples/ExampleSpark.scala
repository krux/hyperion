package com.krux.hyperion.examples

import com.krux.hyperion.DataPipelineDef
import com.krux.hyperion.Implicits._
import com.krux.hyperion.objects.{Schedule, SparkCluster, SparkStep,
  SparkActivity, PipelineObject}
import com.krux.hyperion.expressions.DateTimeFunctions.format
import com.krux.hyperion.expressions.ExpressionDSL._
import com.krux.hyperion.HyperionContext
import com.typesafe.config.ConfigFactory
import com.krux.hyperion.objects.SnsAlarm


class ExampleSpark extends DataPipelineDef {

  val target = "the-target"
  val jar = "s3://sample-jars/sample-jar-assembly-current.jar"

  override implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

  override lazy val schedule = Schedule()
    .startAtActivation
    .period(1.day)

  override def workflow = {

    // Actions
    val mailAction = SnsAlarm("sns-alarm-1")
      .withSubject("Something happened at #{node.@scheduledStartTime}")
      .withMessage("Some message")
      .withTopicArn("arn:aws:sns:us-east-1:28619EXAMPLE:ExampleTopic")
      .withRole("ResourceRole")

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
