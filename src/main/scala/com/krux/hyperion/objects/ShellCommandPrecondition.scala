package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpShellCommandPrecondition

/**
 * A Unix/Linux shell command that can be run as a precondition.
 *
 * @param command The command to run. This value and any associated parameters must function in the environment from which you are running the Task Runner.
 * @param scriptArgument A list of arguments to pass to the shell script.
 * @param scriptUri An Amazon S3 URI path for a file to download and run as a shell command. Only one scriptUri or command field should be present. scriptUri cannot use parameters, use command instead.
 * @param stdout The Amazon S3 path that receives redirected output from the command. If you use the runsOn field, this must be an Amazon S3 path because of the transitory nature of the resource running your activity. However if you specify the workerGroup field, a local file path is permitted.
 * @param stderr The Amazon S3 path that receives redirected system error messages from the command. If you use the runsOn field, this must be an Amazon S3 path because of the transitory nature of the resource running your activity. However if you specify the workerGroup field, a local file path is permitted.
 *
 */
case class ShellCommandPrecondition private (
  id: PipelineObjectId,
  command: String,
  scriptArgument: Seq[String],
  scriptUri: Option[String],
  stdout: Option[String],
  stderr: Option[String],
  preconditionTimeout: Option[String],
  role: Option[String]
)(
  implicit val hc: HyperionContext
) extends Precondition {

  lazy val serialize = AdpShellCommandPrecondition(
    id = id,
    name = id.toOption,
    command = command,
    scriptArgument = scriptArgument,
    scriptUri = scriptUri,
    stdout = stdout,
    stderr = stderr,
    preconditionTimeout = preconditionTimeout,
    role = role.getOrElse(hc.resourceRole)
  )

}

object ShellCommandPrecondition {
  def apply(command: String)(implicit hc: HyperionContext) =
    new ShellCommandPrecondition(
      id = PipelineObjectId("ShellCommandPrecondition"),
      command = command,
      scriptArgument = Seq(),
      scriptUri = None,
      stdout = None,
      stderr = None,
      preconditionTimeout = None,
      role = None
    )
}
