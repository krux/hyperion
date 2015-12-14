package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.{HInt, HDuration, HS3Uri, HString, HBoolean, HType, HDateTime}
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId, ObjectFields, S3Uri}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.ConstantExpression._
import com.krux.hyperion.expression.{Format, DateTimeConstantExp, RunnableObject, Parameter}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

/**
 * Shell command activity that runs a given Jar
 */
case class SftpDownloadActivity private (
  baseFields: ObjectFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  sftpActivityFields: SftpActivityFields,
  scriptUriBase: HString,
  sftpInput: Option[HString]
) extends SftpActivity {

  type Self = SftpDownloadActivity

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)
  def updateSftpActivityFields(fields: SftpActivityFields) = copy(sftpActivityFields = fields)

  def output = shellCommandActivityFields.output
  override private[hyperion] def serializedOutput = output
  def withOutput(outputs: S3DataNode*): Self = updateShellCommandActivityFields(
    shellCommandActivityFields.copy(
      output = shellCommandActivityFields.output ++ outputs,
      stage = Option(HBoolean.True)
    )
  )

  def withInput(in: HString) = copy(sftpInput = Option(in))
  def inputOutput = sftpInput

}

object SftpDownloadActivity extends RunnableObject {

  def apply(host: String)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): SftpDownloadActivity =
    new SftpDownloadActivity(
      baseFields = ObjectFields(PipelineObjectId(SftpDownloadActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(S3Uri(s"${hc.scriptUri}activities/run-jar.sh")),
      sftpActivityFields = SftpActivityFields(host),
      scriptUriBase = hc.scriptUri,
      sftpInput = None
    )

}
