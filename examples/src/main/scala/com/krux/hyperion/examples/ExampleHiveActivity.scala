package com.krux.hyperion.examples

import com.krux.hyperion.{DataPipelineDef, HyperionCli, HyperionContext, Schedule}
import com.krux.hyperion.Implicits._
import com.krux.hyperion.activity.HiveActivity
import com.krux.hyperion.dataformat.CustomDataFormat
import com.krux.hyperion.datanode.S3Folder
import com.krux.hyperion.expression.Parameter
import com.krux.hyperion.resource.MapReduceCluster
import com.typesafe.config.ConfigFactory

object ExampleHiveActivity extends DataPipelineDef with HyperionCli {

  override implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

  override lazy val schedule = Schedule.cron
    .startAtActivation
    .every(1.day)
    .stopAfter(3)

  override lazy val tags = Map("example" -> None, "ownerGroup" -> Some("hive"))

  override def parameters: Iterable[Parameter[_]] = Seq(instanceType, instanceCount, instanceBid)

  val instanceType = Parameter[String]("InstanceType").withValue("c3.8xlarge")
  val instanceCount = Parameter("InstanceCount", 8)
  val instanceBid = Parameter("InstanceBid", 3.40)

  val emrCluster = MapReduceCluster()
    .withTaskInstanceCount(instanceCount)
    .withTaskInstanceType(instanceType)
    .withReleaseLabel("emr-4.4.0")
    .named("Cluster with release label")

  val dataFormat = CustomDataFormat()
    .withColumns(
      "id STRING",
      "a STRING"
    )
    .withColumnSeparator("^")

  val input1 = S3Folder(s3 / "source" / "input1")
    .withDataFormat(dataFormat)

  val input2 = S3Folder(s3 / "source" / "input2")
    .withDataFormat(dataFormat)

  val output = S3Folder(s3 / "dest")
    .withDataFormat(dataFormat)

  val hive = HiveActivity(List(input1, input2), List(output), hiveScript =
    s"""INSERT OVERWRITE TABLE $${output1} SELECT x.a FROM $${input1} x JOIN $${input2} y ON x.id = y.id;"""
    )(emrCluster)

  override def workflow = hive.toWorkflowExpression
}
