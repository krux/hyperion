package com.krux.hyperion.precondition

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.AdpShellCommandPrecondition
import com.krux.hyperion.common.{S3Uri, PipelineObjectId}

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
  script: Either[S3Uri, String],
  scriptArgument: Seq[String],
  stdout: Option[String],
  stderr: Option[String],
  role: String,
  preconditionTimeout: Option[String]
) extends Precondition {

  def withScript(cmd: String) = this.copy(script = Right(cmd))
  def withScript(uri: S3Uri) = this.copy(script = Left(uri))
  def withScriptArgument(argument: String*) = this.copy(scriptArgument = scriptArgument ++ argument)
  def withStdout(stdout: String) = this.copy(stdout = Option(stdout))
  def withStderr(stderr: String) = this.copy(stderr = Option(stderr))
  def withPreconditionTimeout(timeout: String) = this.copy(preconditionTimeout = Option(timeout))
  def withRole(role: String) = this.copy(role = role)

  lazy val serialize = AdpShellCommandPrecondition(
    id = id,
    name = id.toOption,
    command = script.right.toOption,
    scriptUri = script.left.toOption.map(_.ref),
    scriptArgument = scriptArgument,
    stdout = stdout,
    stderr = stderr,
    role = role,
    preconditionTimeout = preconditionTimeout
  )

}

object ShellCommandPrecondition {
  def apply(uri: S3Uri)(implicit hc: HyperionContext): ShellCommandPrecondition = apply(Left(uri))

  def apply(script: String)(implicit hc: HyperionContext): ShellCommandPrecondition = apply(Right(script))

  private def apply(script: Either[S3Uri, String])(implicit hc: HyperionContext): ShellCommandPrecondition =
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
