package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.adt.{ HS3Uri, HString }
import com.krux.hyperion.common.{ BaseFields, PipelineObjectId }
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.resource.{ Ec2Resource, Resource }

/**
 * Google Storage Upload activity
 */
case class GoogleStorageUploadActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  botoConfigUrl: HS3Uri,
  googleStorageUri: HString
) extends GoogleStorageActivity with WithS3Input {

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
