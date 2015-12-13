package com.krux.hyperion.activity

import com.krux.hyperion.adt._
import com.krux.hyperion.adt.HType._
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.ConstantExpression._
import com.krux.hyperion.expression.{ Format, Parameter }
import com.krux.hyperion.resource.Ec2Resource

case class SftpActivityFields(
  host: HString,
  port: Option[HInt] = None,
  username: Option[HString] = None,
  password: Option[Parameter[String]] = None,
  identity: Option[HS3Uri] = None,
  pattern: Option[HString] = None,
  sinceDate: Option[HDateTime] = None,
  untilDate: Option[HDateTime] = None,
  skipEmpty: HBoolean = false,
  markSuccessfulJobs: HBoolean = false
)

trait SftpActivity extends PipelineActivity[Ec2Resource] {

  type Self <: SftpActivity

  def scriptUriBase: HString

  private val DateTimeFormat = "yyyy-MM-dd\\'T\\'HH:mm:ssZZ"

  def shellCommandActivityFields: ShellCommandActivityFields
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields): Self

  def sftpActivityFields: SftpActivityFields
  def updateSftpActivityFields(fields: SftpActivityFields): Self

  require(password.forall(_.isEncrypted), "The password must be an encrypted string parameter")

  val mainClass: HString = "com.krux.hyperion.contrib.activity.sftp.SftpActivity"

  def host = sftpActivityFields.host

  def sinceDate = sftpActivityFields.sinceDate
  def since(date: HDateTime) = updateSftpActivityFields(
    sftpActivityFields.copy(sinceDate = Option(date))
  )

  def untilDate = sftpActivityFields.untilDate
  def until(date: HDateTime) = updateSftpActivityFields(
    sftpActivityFields.copy(untilDate = Option(date))
  )

  def port = sftpActivityFields.port
  def withPort(port: HInt) = updateSftpActivityFields(
    sftpActivityFields.copy(port = Option(port))
  )

  def username = sftpActivityFields.username
  def withUsername(username: HString) = updateSftpActivityFields(
    sftpActivityFields.copy(username = Option(username))
  )

  def password = sftpActivityFields.password
  def withPassword(password: Parameter[String]) = updateSftpActivityFields(
    sftpActivityFields.copy(password = Option(password))
  )

  def identity = sftpActivityFields.identity
  def withIdentity(identity: HS3Uri) = updateSftpActivityFields(
    sftpActivityFields.copy(identity = Option(identity))
  )

  def pattern = sftpActivityFields.pattern
  def withPattern(pattern: HString) = updateSftpActivityFields(
    sftpActivityFields.copy(pattern = Option(pattern))
  )

  def skipEmpty = sftpActivityFields.skipEmpty
  def skippingEmpty() = updateSftpActivityFields(
    sftpActivityFields.copy(skipEmpty = true)
  )

  def markSuccessfulJobs = sftpActivityFields.markSuccessfulJobs
  def markingSuccessfulJobs() = updateSftpActivityFields(
    sftpActivityFields.copy(markSuccessfulJobs = true)
  )

  def stdout = shellCommandActivityFields.stdout
  def withStdoutTo(out: HString): Self = updateShellCommandActivityFields(
    shellCommandActivityFields.copy(stdout = Option(out))
  )

  def stderr = shellCommandActivityFields.stderr
  def withStderrTo(err: HString): Self = updateShellCommandActivityFields(
    shellCommandActivityFields.copy(stderr = Option(err))
  )

  def inputOutput: Option[HString]

  private[hyperion] def serializedInput: Seq[S3DataNode] = Seq.empty

  private[hyperion] def serializedOutput: Seq[S3DataNode] = Seq.empty

  private def arguments: Seq[HType] = Seq(
    Option(Seq[HString]("download")),
    Option(Seq[HString]("--host", host)),
    port.map(p => Seq[HType]("--port", p)),
    username.map(u => Seq[HString]("--user", u)),
    password.map(p => Seq[HString]("--password", p)),
    identity.map(i => Seq[HType]("--identity", i)),
    pattern.map(p => Seq[HString]("--pattern", p)),
    sinceDate.map(d => Seq[HString]("--since", Format(d, DateTimeFormat))),
    untilDate.map(d => Seq[HString]("--until", Format(d, DateTimeFormat))),
    if (skipEmpty) Option(Seq[HString]("--skip-empty")) else None,
    if (markSuccessfulJobs) Option(Seq[HString]("--mark-successful-jobs")) else None,
    Option(inputOutput.toSeq)
  ).flatten.flatten

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = Option(s"${scriptUriBase}activities/run-jar.sh"),
    scriptArgument = Option((jarUri +: mainClass +: arguments).map(_.serialize)),
    stdout = stdout.map(_.serialize),
    stderr = stderr.map(_.serialize),
    stage = Option(HBoolean.True.serialize),
    input = seqToOption(serializedInput)(_.ref),
    output = seqToOption(serializedOutput)(_.ref),
    workerGroup = runsOn.asWorkerGroup.map(_.ref),
    runsOn = runsOn.asManagedResource.map(_.ref),
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref),
    attemptTimeout = attemptTimeout.map(_.serialize),
    lateAfterTimeout = lateAfterTimeout.map(_.serialize),
    maximumRetries = maximumRetries.map(_.serialize),
    retryDelay = retryDelay.map(_.serialize),
    failureAndRerunMode = failureAndRerunMode.map(_.serialize)
  )

  def jarUri: HString = s"${scriptUriBase}activities/hyperion-sftp-activity-current-assembly.jar"

}
