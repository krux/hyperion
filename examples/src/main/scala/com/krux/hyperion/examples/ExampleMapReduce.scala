package com.krux.hyperion.examples

import scala.language.postfixOps
import com.krux.hyperion.Implicits._
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.activity._
import com.krux.hyperion.common.S3Uri
import com.krux.hyperion.expression.{Format, Parameter, RuntimeNode}
import com.krux.hyperion.resource.{MapReduceCluster}
import com.krux.hyperion.{DataPipelineDef, HyperionContext, Schedule, _}
import com.typesafe.config.ConfigFactory

object ExampleMapReduce extends DataPipelineDef with HyperionCli {

  val target = "the-target"
  val jar = s3 / "sample-jars" / "sample-jar-assembly-current.jar"

  override implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

  override lazy val tags = Map("example" -> None, "ownerGroup" -> Some("mapReduce"))

  override lazy val schedule = Schedule.cron
    .startAtActivation
    .every(1.day)
    .stopAfter(3)

  val location = Parameter[S3Uri]("S3Location").withValue(s3"your-location")
  val instanceType = Parameter[String]("InstanceType").withValue("c3.8xlarge")
  val instanceCount = Parameter("InstanceCount", 8)
  val instanceBid = Parameter("InstanceBid", 3.40)

  override def parameters: Iterable[Parameter[_]] = Seq(location, instanceType, instanceCount, instanceBid)

  // Actions
  val mailAction = SnsAlarm("arn:aws:sns:us-east-1:28619EXAMPLE:ExampleTopic")
    .withSubject(s"Something happened at ${RuntimeNode.ScheduledStartTime}")
    .withMessage(s"Some message $instanceCount x $instanceType @ $instanceBid for $location")
    .withRole("DataPipelineDefaultResourceRole")

  // Resources
  val emrCluster = MapReduceCluster()
    .withTaskInstanceCount(instanceCount)
    .withTaskInstanceType(instanceType)
    .withReleaseLabel("emr-4.4.0")
    .named("Cluster with release label")

  // First activity
  val filterActivity = MapReduceActivity(emrCluster)
    .named("filterActivity")
    .onFail(mailAction)
    .withSteps(
      MapReduceStep(jar)
        .withMainClass("com.krux.hyperion.ScoreJob1")
        .withArguments(
          target,
          Format(SparkActivity.ScheduledStartTime - 3.days, "yyyy-MM-dd")
        )
    )

  // Second activity
  val scoreActivity = MapReduceActivity(emrCluster)
    .named("scoreActivity")
    .onSuccess(mailAction)
    .withSteps(
      MapReduceStep(jar)
        .withMainClass("com.krux.hyperion.ScoreJob2")
        .withArguments(
          target,
      Format(SparkActivity.ScheduledStartTime - 3.days, "yyyy-MM-dd"),
      "denormalized"
        )
    )

  override def workflow = filterActivity ~> scoreActivity

}

