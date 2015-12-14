package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.HType._
import com.krux.hyperion.adt.{HInt, HDuration, HS3Uri, HString, HBoolean}
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{ PipelineObject, PipelineObjectId, ObjectFields, S3Uri }
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

/**
 * Google Storage Download activity
 */
case class GoogleStorageDownloadActivity private (
  baseFields: ObjectFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  botoConfigUrl: HS3Uri,
  gsInput: HString
) extends GoogleStorageActivity {

  type Self = GoogleStorageDownloadActivity

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

  override private[hyperion] def serializedOutput = output
  def output = shellCommandActivityFields.output
  def withOutput(outputs: S3DataNode*): Self = updateShellCommandActivityFields(
    shellCommandActivityFields.copy(
      output = shellCommandActivityFields.output ++ outputs,
      stage = Option(HBoolean.True)
    )
  )

  override def scriptArguments = Seq(botoConfigUrl.serialize: HString, gsInput)

}

object GoogleStorageDownloadActivity extends RunnableObject {

  def apply(botoConfigUrl: HS3Uri)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): GoogleStorageDownloadActivity =
    new GoogleStorageDownloadActivity(
      baseFields = ObjectFields(PipelineObjectId(GoogleStorageDownloadActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(S3Uri(s"${hc.scriptUri}activities/gsutil-download.sh")),
      botoConfigUrl = botoConfigUrl,
      gsInput = ""
    )
}
