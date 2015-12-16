package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.HType._
import com.krux.hyperion.adt.{ HInt, HDuration, HS3Uri, HString, HBoolean }
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{ PipelineObject, PipelineObjectId, BaseFields, S3Uri }
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{ Resource, Ec2Resource }

/**
 * Google Storage Upload activity
 */
case class GoogleStorageUploadActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  botoConfigUrl: HS3Uri,
  googleStorageUri: HString
) extends GoogleStorageActivity with WithS3Output {

  type Self = GoogleStorageUploadActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

}

object GoogleStorageUploadActivity extends RunnableObject {

  def apply(botoConfigUrl: HS3Uri, output: HString)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): GoogleStorageUploadActivity =
    new GoogleStorageUploadActivity(
      baseFields = BaseFields(PipelineObjectId(GoogleStorageUploadActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(GoogleStorageActivity.uploadScript),
      botoConfigUrl = botoConfigUrl,
      googleStorageUri = output
    )

}
