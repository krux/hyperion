package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.{HInt, HDuration, HS3Uri, HString, HBoolean, HType, HDateTime}
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId, BaseFields, S3Uri}
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
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  sftpActivityFields: SftpActivityFields,
  scriptUriBase: HString,
  sftpPath: Option[HString]
) extends SftpActivity with WithS3Output {

  type Self = SftpDownloadActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)
  def updateSftpActivityFields(fields: SftpActivityFields) = copy(sftpActivityFields = fields)

  def withInput(in: HString) = copy(sftpPath = Option(in))

}

object SftpDownloadActivity extends RunnableObject {

  def apply(host: String)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): SftpDownloadActivity =
    new SftpDownloadActivity(
      baseFields = BaseFields(PipelineObjectId(SftpDownloadActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(S3Uri(s"${hc.scriptUri}activities/run-jar.sh")),
      sftpActivityFields = SftpActivityFields(host),
      scriptUriBase = hc.scriptUri,
      sftpPath = None
    )

}
