package com.krux.hyperion.examples

import com.krux.hyperion.common.S3Uri
import com.krux.hyperion.expression.Parameter
import com.krux.hyperion.{DataPipelineDef, HyperionCli, HyperionContext, Schedule}
import com.typesafe.config.ConfigFactory
import com.krux.hyperion.Implicits._
import com.krux.hyperion.activity.S3DistCpActivity
import com.krux.hyperion.activity.S3DistCpActivity.OutputCodec
import com.krux.hyperion.resource.MapReduceCluster

object ExampleS3DistCpWorkflow extends DataPipelineDef with HyperionCli {

  val source = "the-source"
  val target = hdfs / "" / "the-target"
  val jar = s3 / "sample-jars" / "sample-jar-assembly-current.jar"

  override implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

  override lazy val tags = Map("example" -> None, "ownerGroup" -> Some("s3DistCP"))

  override lazy val schedule = Schedule.cron
    .startAtActivation
    .every(1.day)
    .stopAfter(3)

  val location = Parameter[S3Uri]("S3Location").withValue(s3"your-location")
  val instanceType = Parameter[String]("InstanceType").withValue("c3.8xlarge")
  val instanceCount = Parameter("InstanceCount", 8)
  val instanceBid = Parameter("InstanceBid", 3.40)

  override def parameters: Iterable[Parameter[_]] = Seq(location, instanceType, instanceCount, instanceBid)

  // Resources
  val emrCluster = MapReduceCluster()
    .withTaskInstanceCount(instanceCount)
    .withTaskInstanceType(instanceType)
    .withReleaseLabel("emr-4.4.0")
    .named("Cluster with release label")

  val s3DistCPActivity = S3DistCpActivity(emrCluster)
    .named("s3DistCpActivity")
    .withSource(
      s3 / source
    )
    .withDestination(
      target
    )
    .withOutputCodec(
      OutputCodec.Gz
    )

  override def workflow = s3DistCPActivity.toWorkflowExpression
}
