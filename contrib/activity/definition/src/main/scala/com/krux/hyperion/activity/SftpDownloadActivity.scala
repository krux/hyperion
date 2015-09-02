package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{S3Uri, PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.{DateTimeFunctions, DateTimeExp, Duration}
import com.krux.hyperion.parameter.{Parameter, StringParameter}
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

/**
 * Shell command activity that runs a given Jar
 */
class SftpDownloadActivity private (
  val id: PipelineObjectId,
  val scriptUri: Option[String],
  val jarUri: String,
  val mainClass: String,
  val host: String,
  val port: Option[Parameter[Int]],
  val username: Option[String],
  val password: Option[StringParameter],
  val identity: Option[Parameter[S3Uri]],
  val pattern: Option[String],
  val sinceDate: Option[DateTimeExp],
  val untilDate: Option[DateTimeExp],
  val skipEmpty: Boolean,
  val markSuccessfulJobs: Boolean,
  val input: Option[String],
  val output: Option[S3DataNode],
  val stdout: Option[String],
  val stderr: Option[String],
  val runsOn: Resource[Ec2Resource],
  val dependsOn: Seq[PipelineActivity],
  val preconditions: Seq[Precondition],
  val onFailAlarms: Seq[SnsAlarm],
  val onSuccessAlarms: Seq[SnsAlarm],
  val onLateActionAlarms: Seq[SnsAlarm],
  val attemptTimeout: Option[Parameter[Duration]],
  val lateAfterTimeout: Option[Parameter[Duration]],
  val maximumRetries: Option[Parameter[Int]],
  val retryDelay: Option[Parameter[Duration]],
  val failureAndRerunMode: Option[FailureAndRerunMode]
) extends SftpActivity {

  require(password.forall(_.isEncrypted), "The password must be an encrypted string parameter")

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def since(date: DateTimeExp) = this.copy(sinceDate = Option(date))
  def until(date: DateTimeExp) = this.copy(untilDate = Option(date))
  def withPort(port: Parameter[Int]) = this.copy(port = Option(port))
  def withUsername(username: String) = this.copy(username = Option(username))
  def withPassword(password: StringParameter) = this.copy(password = Option(password))
  def withIdentity(identity: Parameter[S3Uri]) = this.copy(identity = Option(identity))
  def withPattern(pattern: String) = this.copy(pattern = Option(pattern))
  def skippingEmpty() = this.copy(skipEmpty = true)
  def markingSuccessfulJobs() = this.copy(markSuccessfulJobs = true)
  def withInput(input: String) = this.copy(input = Option(input))
  def withOutput(output: S3DataNode) = this.copy(output = Option(output))
  def withStdoutTo(out: String) = this.copy(stdout = Option(out))
  def withStderrTo(err: String) = this.copy(stderr = Option(err))

  private[hyperion] def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)
  def withAttemptTimeout(timeout: Parameter[Duration]) = this.copy(attemptTimeout = Option(timeout))
  def withLateAfterTimeout(timeout: Parameter[Duration]) = this.copy(lateAfterTimeout = Option(timeout))
  def withMaximumRetries(retries: Parameter[Int]) = this.copy(maximumRetries = Option(retries))
  def withRetryDelay(delay: Parameter[Duration]) = this.copy(retryDelay = Option(delay))
  def withFailureAndRerunMode(mode: FailureAndRerunMode) = this.copy(failureAndRerunMode = Option(mode))

  def copy(
    id: PipelineObjectId = id,
    scriptUri: Option[String] = scriptUri,
    jarUri: String = jarUri,
    mainClass: String = mainClass,
    host: String = host,
    port: Option[Parameter[Int]] = port,
    username: Option[String] = username,
    password: Option[StringParameter] = password,
    identity: Option[Parameter[S3Uri]] = identity,
    pattern: Option[String] = pattern,
    sinceDate: Option[DateTimeExp] = sinceDate,
    untilDate: Option[DateTimeExp] = untilDate,
    skipEmpty: Boolean = skipEmpty,
    markSuccessfulJobs: Boolean = markSuccessfulJobs,
    input: Option[String] = input,
    output: Option[S3DataNode] = output,
    stdout: Option[String] = stdout,
    stderr: Option[String] = stderr,
    runsOn: Resource[Ec2Resource] = runsOn,
    dependsOn: Seq[PipelineActivity] = dependsOn,
    preconditions: Seq[Precondition] = preconditions,
    onFailAlarms: Seq[SnsAlarm] = onFailAlarms,
    onSuccessAlarms: Seq[SnsAlarm] = onSuccessAlarms,
    onLateActionAlarms: Seq[SnsAlarm] = onLateActionAlarms,
    attemptTimeout: Option[Parameter[Duration]] = attemptTimeout,
    lateAfterTimeout: Option[Parameter[Duration]] = lateAfterTimeout,
    maximumRetries: Option[Parameter[Int]] = maximumRetries,
    retryDelay: Option[Parameter[Duration]] = retryDelay,
    failureAndRerunMode: Option[FailureAndRerunMode] = failureAndRerunMode
  ) = new SftpDownloadActivity(
    id, scriptUri, jarUri, mainClass, host, port, username, password, identity, pattern, sinceDate, untilDate,
    skipEmpty, markSuccessfulJobs, input, output, stdout, stderr, runsOn, dependsOn, preconditions,
    onFailAlarms, onSuccessAlarms, onLateActionAlarms,
    attemptTimeout, lateAfterTimeout, maximumRetries, retryDelay, failureAndRerunMode
  )

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  private val DateTimeFormat = "yyyy-MM-dd\\'T\\'HH:mm:ssZZ"

  private def arguments: Seq[String] = Seq(
    Option(Seq("download")),
    Option(Seq("--host", host)),
    port.map(p => Seq("--port", p.toString)),
    username.map(u => Seq("--user", u)),
    password.map(p => Seq("--password", p.toString)),
    identity.map(i => Seq("--identity", i.toString)),
    pattern.map(p => Seq("--pattern", p)),
    sinceDate.map(d => Seq("--since", DateTimeFunctions.format(d, DateTimeFormat).toString)),
    untilDate.map(d => Seq("--until", DateTimeFunctions.format(d, DateTimeFormat).toString)),
    if (skipEmpty) Option(Seq("--skip-empty")) else None,
    if (markSuccessfulJobs) Option(Seq("--mark-successful-jobs")) else None,
    input.map(in => Seq(in))
  ).flatten.flatten

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = scriptUri,
    scriptArgument = Option(Seq(jarUri, mainClass) ++ arguments),
    stdout = stdout,
    stderr = stderr,
    stage = Option("true"),
    input = None,
    output = output.map(o => Seq(o.ref)),
    workerGroup = runsOn.asWorkerGroup.map(_.ref),
    runsOn = runsOn.asManagedResource.map(_.ref),
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref),
    attemptTimeout = attemptTimeout.map(_.toString),
    lateAfterTimeout = lateAfterTimeout.map(_.toString),
    maximumRetries = maximumRetries.map(_.toString),
    retryDelay = retryDelay.map(_.toString),
    failureAndRerunMode = failureAndRerunMode.map(_.toString)
  )

}

object SftpDownloadActivity extends RunnableObject {

  def apply(host: String)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): SftpDownloadActivity =
    new SftpDownloadActivity(
      id = PipelineObjectId(SftpDownloadActivity.getClass),
      scriptUri = Option(s"${hc.scriptUri}activities/run-jar.sh"),
      jarUri = s"${hc.scriptUri}activities/hyperion-sftp-activity-current-assembly.jar",
      mainClass = "com.krux.hyperion.contrib.activity.sftp.SftpActivity",
      host = host,
      port = None,
      username = None,
      password = None,
      identity = None,
      pattern = None,
      sinceDate = None,
      untilDate = None,
      skipEmpty = false,
      markSuccessfulJobs = false,
      input = None,
      output = None,
      stdout = None,
      stderr = None,
      runsOn = runsOn,
      dependsOn = Seq.empty,
      preconditions = Seq.empty,
      onFailAlarms = Seq.empty,
      onSuccessAlarms = Seq.empty,
      onLateActionAlarms = Seq.empty,
      attemptTimeout = None,
      lateAfterTimeout = None,
      maximumRetries = None,
      retryDelay = None,
      failureAndRerunMode = None
    )

}
