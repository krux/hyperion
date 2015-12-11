package com.krux.hyperion.h3.precondition

import com.krux.hyperion.h3.activity.Script
import com.krux.hyperion.adt.{HDuration, HString}
import com.krux.hyperion.aws.AdpShellCommandPrecondition
import com.krux.hyperion.h3.common.{ PipelineObjectId, ObjectFields }
import com.krux.hyperion.HyperionContext

/**
 * A Unix/Linux shell command that can be run as a precondition.
 *
 * @param script The command to run or an Amazon S3 URI path for a file to download and run as a shell command. scriptUri cannot use parameters, use command instead.
 * @param scriptArgument A list of arguments to pass to the shell script.
 * @param stdout The Amazon S3 path that receives redirected output from the command. If you use the runsOn field, this must be an Amazon S3 path because of the transitory nature of the resource running your activity. However if you specify the workerGroup field, a local file path is permitted.
 * @param stderr The Amazon S3 path that receives redirected system error messages from the command. If you use the runsOn field, this must be an Amazon S3 path because of the transitory nature of the resource running your activity. However if you specify the workerGroup field, a local file path is permitted.
 *
 */
case class ShellCommandPrecondition private (
  baseFields: ObjectFields,
  preconditionFields: PreconditionFields,
  script: Script,
  scriptArgument: Seq[HString],
  stdout: Option[HString],
  stderr: Option[HString]
) extends Precondition {

  type Self = ShellCommandPrecondition

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updatePreconditionFields(fields: PreconditionFields) = copy(preconditionFields = fields)

  def withScriptArgument(argument: HString*) = this.copy(scriptArgument = scriptArgument ++ argument)
  def withStdout(stdout: HString) = this.copy(stdout = Option(stdout))
  def withStderr(stderr: HString) = this.copy(stderr = Option(stderr))

  lazy val serialize = AdpShellCommandPrecondition(
    id = id,
    name = id.toOption,
    command = script.content.map(_.serialize),
    scriptUri = script.uri.map(_.serialize),
    scriptArgument = scriptArgument.map(_.serialize),
    stdout = stdout.map(_.serialize),
    stderr = stderr.map(_.serialize),
    role = role.serialize,
    preconditionTimeout = preconditionTimeout.map(_.serialize)
  )

}

object ShellCommandPrecondition {

  def apply(script: Script)(implicit hc: HyperionContext): ShellCommandPrecondition =
    new ShellCommandPrecondition(
      baseFields = ObjectFields(PipelineObjectId(ShellCommandPrecondition.getClass)),
      preconditionFields = Precondition.defaultPreconditionFields,
      script = script,
      scriptArgument = Seq.empty,
      stdout = None,
      stderr = None
    )

}
