package com.krux.hyperion.examples

import com.krux.hyperion.activity.RedshiftCopyActivity
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.database.RedshiftDatabase
import com.krux.hyperion.dataformat.TsvDataFormat
import com.krux.hyperion.datanode.{S3DataNode, RedshiftDataNode}
import com.krux.hyperion.Implicits._
import com.krux.hyperion.resource.Ec2Resource
import com.krux.hyperion.WorkflowExpression._
import com.krux.hyperion.{Schedule, DataPipelineDef, HyperionContext}
import com.typesafe.config.ConfigFactory

/**
 * An example redshift loader object
 */
object ExampleRedshiftLoad extends DataPipelineDef {

  object MockRedshift extends RedshiftDatabase {
    val id = PipelineObjectId.fixed("_MockRedshift")
    val name = id
    val clusterId = "mock-redshift"
    val username = "mockuser"
    val `*password` = "mockpass"
    val databaseName = "mock_db"
  }

  override implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

  override lazy val schedule = Schedule.cron
    .startAtActivation
    .every(1.hour)

  val ec2Instance = Ec2Resource()

  val s3Format = TsvDataFormat()

  val redshiftTable = RedshiftDataNode(
    MockRedshift,
    "monthly_campaign_frequency_distribution"
  )
    .withSchema("kexin")
    .withPrimaryKeys("publisher_id", "campaign_id", "month")

  override def workflow =
    RedshiftCopyActivity(
      input = S3DataNode(s3"testing/testtab/").withDataFormat(s3Format),
      output = redshiftTable,
      insertMode = RedshiftCopyActivity.OverwriteExisting
    )(ec2Instance)

}
