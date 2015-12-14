package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.{ HInt, HDuration, HS3Uri, HString, HBoolean, HType }
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{ PipelineObject, PipelineObjectId, ObjectFields, S3Uri }
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.{ RunnableObject, Parameter }
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{ Resource, Ec2Resource }

/**
 * Shell command activity that runs a given Jar
 */
case class SftpUploadActivity private (
  baseFields: ObjectFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  sftpActivityFields: SftpActivityFields,
  scriptUriBase: HString,
  sftpOutput: Option[HString]
) extends SftpActivity {

  type Self = SftpUploadActivity

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)
  def updateSftpActivityFields(fields: SftpActivityFields) = copy(sftpActivityFields = fields)

  def input = shellCommandActivityFields.input
  override private[hyperion] def serializedInput = input
  def withInput(inputs: S3DataNode*): Self = updateShellCommandActivityFields(
    shellCommandActivityFields.copy(
      input = shellCommandActivityFields.input ++ input,
      stage = Option(HBoolean.True)
    )
  )

  def withOutput(out: HString) = copy(sftpOutput = Option(out))
  def inputOutput = sftpOutput

}

object SftpUploadActivity extends RunnableObject {

  def apply(host: String)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): SftpUploadActivity =
    new SftpUploadActivity(
      baseFields = ObjectFields(PipelineObjectId(SftpUploadActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(S3Uri(s"${hc.scriptUri}activities/run-jar.sh")),
      sftpActivityFields = SftpActivityFields(host),
      scriptUriBase = hc.scriptUri,
      sftpOutput = None
    )

}
