package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.HType._
import com.krux.hyperion.adt.{ HInt, HDuration, HS3Uri, HString, HBoolean }
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{ PipelineObject, PipelineObjectId, ObjectFields, S3Uri }
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{ Resource, Ec2Resource }

/**
 * Google Storage Upload activity
 */
case class GoogleStorageUploadActivity private (
  baseFields: ObjectFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  botoConfigUrl: HS3Uri,
  googleStorageUri: HString
) extends GoogleStorageActivity with WithS3Output {

  type Self = GoogleStorageUploadActivity

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

  def withOutput(out: HString): Self = copy(googleStorageUri = out)

}

object GoogleStorageUploadActivity extends RunnableObject {

  def apply(botoConfigUrl: HS3Uri)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): GoogleStorageUploadActivity =
    new GoogleStorageUploadActivity(
      baseFields = ObjectFields(PipelineObjectId(GoogleStorageUploadActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(GoogleStorageActivity.uploadScript),
      botoConfigUrl = botoConfigUrl,
      googleStorageUri = ""
    )

}
