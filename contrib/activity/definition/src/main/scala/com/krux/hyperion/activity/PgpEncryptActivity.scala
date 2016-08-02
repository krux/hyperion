package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.adt.{HBoolean, HS3Uri}
import com.krux.hyperion.common.{BaseFields, PipelineObjectId}
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.resource.{Ec2Resource, Resource}

/**
  * The server-side PGP encryption activity encrypts files from the input location to the output location using the
  * public encryption key.
  *
  * @param baseFields the pipeline base fields
  * @param activityFields the activity setup fields
  * @param shellCommandActivityFields the shell command setup fields
  * @param key the file containing the public encryption key
  * @param markSuccessfulJobs add a _SUCCESS file to the output location on success
  */
case class PgpEncryptActivity private(
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  key: HS3Uri,
  markSuccessfulJobs: HBoolean
) extends PgpActivity {
  type Self = PgpEncryptActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)

  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)

  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

  def markOnSuccess = copy(markSuccessfulJobs = true)

  override def scriptArguments = Seq(
    if (markSuccessfulJobs) Option("--mark-successful-jobs") else None,
    Option(key.serialize)
  ).flatten
}

object PgpEncryptActivity extends RunnableObject {
  def apply(key: HS3Uri)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): PgpEncryptActivity =
    new PgpEncryptActivity(
      baseFields = BaseFields(PipelineObjectId(PgpEncryptActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(PgpActivity.encryptScript),
      key = key,
      markSuccessfulJobs = HBoolean.False
    )
}
