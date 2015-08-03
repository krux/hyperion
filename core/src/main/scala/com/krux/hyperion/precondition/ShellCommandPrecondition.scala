package com.krux.hyperion.precondition

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.activity.Script
import com.krux.hyperion.aws.AdpShellCommandPrecondition
import com.krux.hyperion.common.{S3Uri, PipelineObjectId}
import com.krux.hyperion.expression.Duration
import com.krux.hyperion.parameter.Parameter

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
  id: PipelineObjectId,
  script: Script,
  scriptArgument: Seq[String],
  stdout: Option[String],
  stderr: Option[String],
  role: String,
  preconditionTimeout: Option[Parameter[Duration]]
) extends Precondition {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withScriptArgument(argument: String*) = this.copy(scriptArgument = scriptArgument ++ argument)
  def withStdout(stdout: String) = this.copy(stdout = Option(stdout))
  def withStderr(stderr: String) = this.copy(stderr = Option(stderr))
  def withRole(role: String) = this.copy(role = role)
  def withPreconditionTimeout(timeout: Parameter[Duration]) = this.copy(preconditionTimeout = Option(timeout))

  lazy val serialize = AdpShellCommandPrecondition(
    id = id,
    name = id.toOption,
    command = script.content,
    scriptUri = script.uri.map(_.ref),
    scriptArgument = scriptArgument,
    stdout = stdout,
    stderr = stderr,
    role = role,
    preconditionTimeout = preconditionTimeout.map(_.toString)
  )

}

object ShellCommandPrecondition {
  def apply(script: Script)(implicit hc: HyperionContext): ShellCommandPrecondition =
    new ShellCommandPrecondition(
      id = PipelineObjectId(ShellCommandPrecondition.getClass),
      script = script,
      scriptArgument = Seq(),
      stdout = None,
      stderr = None,
      role = hc.role,
      preconditionTimeout = None
    )
}
