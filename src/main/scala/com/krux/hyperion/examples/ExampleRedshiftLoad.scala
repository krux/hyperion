package com.krux.hyperion.examples

import com.krux.hyperion.objects._
import com.krux.hyperion.Implicits._
import com.krux.hyperion.DataPipelineDef
import com.krux.hyperion.HyperionContext
import com.typesafe.config.ConfigFactory

/**
 * An example redshift loader object
 */
object ExampleRedshiftLoad extends DataPipelineDef {

  object MockRedshift extends RedshiftDatabase {
    val id = "_MockRedshift"
    val name = id
    val clusterId = "mock-redshift"
    val username = "mockuser"
    val `*password` = "mockpass"
    val databaseName = "mock_db"
  }

  override implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

  override lazy val schedule = Schedule()
    .startAtActivation
    .every(1.hour)

  override def workflow = {

    val ec2instance = Ec2Resource()

    val s3Format = TsvDataFormat()

    val redshiftTable = RedshiftDataNode(
        "destTable",
        MockRedshift,
        "monthly_campaign_frequency_distribution"
      )
      .withSchema("kexin")
      .withPrimaryKeys("publisher_id", "campaign_id", "month")

    Some(RedshiftCopyActivity(
      id = "copy",
      input = S3DataNode.fromPath("s3://testing/testtab/").withDataFormat(s3Format),
      insertMode = RedshiftCopyActivity.OverwriteExisting,
      runsOn = ec2instance,
      output = redshiftTable
    ))
  }

}
