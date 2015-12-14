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
  gsOutput: HString
) extends GoogleStorageActivity {

  type Self = GoogleStorageUploadActivity

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

  override private[hyperion] def serializedInput = input
  def input = shellCommandActivityFields.input
  def withInput(inputs: S3DataNode*): Self = updateShellCommandActivityFields(
    shellCommandActivityFields.copy(
      input = shellCommandActivityFields.input ++ inputs,
      stage = Option(HBoolean.True)
    )
  )

  override def scriptArguments = Seq(botoConfigUrl.serialize: HString, gsOutput)

}

object GoogleStorageUploadActivity extends RunnableObject {

  def apply(botoConfigUrl: HS3Uri)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): GoogleStorageUploadActivity =
    new GoogleStorageUploadActivity(
      baseFields = ObjectFields(PipelineObjectId(GoogleStorageUploadActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(S3Uri(s"${hc.scriptUri}activities/gsutil-upload.sh")),
      botoConfigUrl = botoConfigUrl,
      gsOutput = ""
    )

}
