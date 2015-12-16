package com.krux.hyperion.activity

import com.krux.hyperion.adt.HType._
import com.krux.hyperion.adt.{ HS3Uri, HString, HBoolean }
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{ PipelineObjectId, BaseFields, S3Uri }
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

/**
 * Google Storage Download activity
 */
case class GoogleStorageDownloadActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  botoConfigUrl: HS3Uri,
  googleStorageUri: HString
) extends GoogleStorageActivity with WithS3Output {

  type Self = GoogleStorageDownloadActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

}

object GoogleStorageDownloadActivity extends RunnableObject {

  def apply(botoConfigUrl: HS3Uri, input: HString)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): GoogleStorageDownloadActivity =
    new GoogleStorageDownloadActivity(
      baseFields = BaseFields(PipelineObjectId(GoogleStorageDownloadActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(GoogleStorageActivity.downloadScript),
      botoConfigUrl = botoConfigUrl,
      googleStorageUri = input
    )

}
