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
case class ShellCommandPrecondition(
  id: String,
  command: String,
  scriptArgument: Seq[String] = Seq(),
  scriptUri: Option[String] = None,
  stdout: Option[String] = None,
  stderr: Option[String] = None,
  preconditionTimeout: Option[String] = None,
  role: Option[String] = None
)(
  implicit val hc: HyperionContext
) extends Precondition {

  def serialize = AdpShellCommandPrecondition(
    id = id,
    name = Some(id),
    command = command,
    scriptArgument = scriptArgument match {
      case Seq() => None
      case arguments => Some(arguments)
    },
    scriptUri = scriptUri,
    stdout = stdout,
    stderr = stderr,
    preconditionTimeout = preconditionTimeout,
    role = role.getOrElse(hc.resourceRole)
  )

}


