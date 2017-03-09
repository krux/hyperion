package com.krux.hyperion.examples

import com.typesafe.config.ConfigFactory

import com.krux.hyperion.activity.GoogleStorageUploadActivity
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.{DataPipelineDef, HyperionCli, HyperionContext, RecurringSchedule, Schedule}
import com.krux.hyperion.resource.Ec2Resource
import com.krux.hyperion.workflow.WorkflowExpression


object ExampleGoogleUploadActivity extends DataPipelineDef with HyperionCli {

  override implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

  override lazy val schedule: RecurringSchedule = Schedule.cron
    .startAtActivation

  override lazy val tags = Map("example" -> None, "ownerGroup" -> Some("googleActivity"))

  val inputData: S3DataNode = s3 / "the_source"

  val ec2Instance = Ec2Resource()

  val uploadActivity: GoogleStorageUploadActivity = GoogleStorageUploadActivity(
    s3 / "the config location",
    "gs://upload_location"
  )(ec2Instance)
    .named("Google Upload Activity")
    .withInput(
      inputData
    )

  val uploadActivityWithRecursive: GoogleStorageUploadActivity = GoogleStorageUploadActivity(
    s3 / "the config location",
    "gs://upload_location"
  )(ec2Instance)
    .named("Google Upload Activity - Recursive")
    .withInput(
      inputData
    )
    .withRecursive

  override def workflow: WorkflowExpression = uploadActivity ~> uploadActivityWithRecursive

}
