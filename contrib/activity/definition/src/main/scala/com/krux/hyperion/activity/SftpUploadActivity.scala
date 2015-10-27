package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.{HInt, HDuration, HS3Uri, HString, HBoolean, HType}
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.{RunnableObject, Parameter}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

/**
 * Shell command activity that runs a given Jar
 */
class SftpUploadActivity private (
  val id: PipelineObjectId,
  val scriptUri: Option[HString],
  val jarUri: HString,
  val mainClass: HString,
  val host: HString,
  val port: Option[HInt],
  val username: Option[HString],
  val password: Option[Parameter[String]],
  val identity: Option[HS3Uri],
  val pattern: Option[HString],
  val skipEmpty: HBoolean,
  val markSuccessfulJobs: HBoolean,
  val input: Option[S3DataNode],
  val output: Option[HString],
  val stdout: Option[HString],
  val stderr: Option[HString],
  val runsOn: Resource[Ec2Resource],
  val dependsOn: Seq[PipelineActivity],
  val preconditions: Seq[Precondition],
  val onFailAlarms: Seq[SnsAlarm],
  val onSuccessAlarms: Seq[SnsAlarm],
  val onLateActionAlarms: Seq[SnsAlarm],
  val attemptTimeout: Option[HDuration],
  val lateAfterTimeout: Option[HDuration],
  val maximumRetries: Option[HInt],
  val retryDelay: Option[HDuration],
  val failureAndRerunMode: Option[FailureAndRerunMode]
) extends SftpActivity {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withPort(port: HInt) = this.copy(port = Option(port))
  def withUsername(username: HString) = this.copy(username = Option(username))
  def withPassword(password: Parameter[String]) = this.copy(password = Option(password))
  def withIdentity(identity: HS3Uri) = this.copy(identity = Option(identity))
  def withPattern(pattern: HString) = this.copy(pattern = Option(pattern))
  def skippingEmpty() = this.copy(skipEmpty = true)
  def markingSuccessfulJobs() = this.copy(markSuccessfulJobs = true)
  def withInput(input: S3DataNode) = this.copy(input = Option(input))
  def withOutput(output: HString) = this.copy(output = Option(output))
  def withStdoutTo(out: HString) = this.copy(stdout = Option(out))
  def withStderrTo(err: HString) = this.copy(stderr = Option(err))

  private[hyperion] def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)
  def withAttemptTimeout(timeout: HDuration) = this.copy(attemptTimeout = Option(timeout))
  def withLateAfterTimeout(timeout: HDuration) = this.copy(lateAfterTimeout = Option(timeout))
  def withMaximumRetries(retries: HInt) = this.copy(maximumRetries = Option(retries))
  def withRetryDelay(delay: HDuration) = this.copy(retryDelay = Option(delay))
  def withFailureAndRerunMode(mode: FailureAndRerunMode) = this.copy(failureAndRerunMode = Option(mode))

  def copy(
    id: PipelineObjectId = id,
    scriptUri: Option[HString] = scriptUri,
    jarUri: HString = jarUri,
    mainClass: HString = mainClass,
    host: HString = host,
    port: Option[HInt] = port,
    username: Option[HString] = username,
    password: Option[Parameter[String]] = password,
    identity: Option[HS3Uri] = identity,
    pattern: Option[HString] = pattern,
    skipEmpty: HBoolean = skipEmpty,
    markSuccessfulJobs: HBoolean = markSuccessfulJobs,
    input: Option[S3DataNode] = input,
    output: Option[HString] = output,
    stdout: Option[HString] = stdout,
    stderr: Option[HString] = stderr,
    runsOn: Resource[Ec2Resource] = runsOn,
    dependsOn: Seq[PipelineActivity] = dependsOn,
    preconditions: Seq[Precondition] = preconditions,
    onFailAlarms: Seq[SnsAlarm] = onFailAlarms,
    onSuccessAlarms: Seq[SnsAlarm] = onSuccessAlarms,
    onLateActionAlarms: Seq[SnsAlarm] = onLateActionAlarms,
    attemptTimeout: Option[HDuration] = attemptTimeout,
    lateAfterTimeout: Option[HDuration] = lateAfterTimeout,
    maximumRetries: Option[HInt] = maximumRetries,
    retryDelay: Option[HDuration] = retryDelay,
    failureAndRerunMode: Option[FailureAndRerunMode] = failureAndRerunMode
  ) = new SftpUploadActivity(
    id, scriptUri, jarUri, mainClass, host, port, username, password, identity, pattern, skipEmpty, markSuccessfulJobs,
    input, output, stdout, stderr, runsOn, dependsOn, preconditions,
    onFailAlarms, onSuccessAlarms, onLateActionAlarms, attemptTimeout,
    lateAfterTimeout, maximumRetries, retryDelay, failureAndRerunMode
  )

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ input ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  private def arguments: Seq[HType] = Seq(
    Option(Seq[HString]("upload")),
    Option(Seq[HString]("--host", host)),
    port.map(p => Seq[HType]("--port", p)),
    username.map(u => Seq[HString]("--user", u)),
    password.map(p => Seq[HType]("--password", p)),
    identity.map(i => Seq[HType]("--identity", i)),
    pattern.map(p => Seq[HString]("--pattern", p)),
    if (skipEmpty) Option(Seq[HString]("--skip-empty")) else None,
    if (markSuccessfulJobs) Option(Seq[HString]("--mark-successful-jobs")) else None,
    output.map(out => Seq[HString](out))
  ).flatten.flatten

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = scriptUri.map(_.serialize),
    scriptArgument = Option((jarUri +: mainClass +: arguments).map(_.serialize)),
    stdout = stdout.map(_.serialize),
    stderr = stderr.map(_.serialize),
    stage = Option(HBoolean.True.serialize),
    input = input.map(i => Seq(i.ref)),
    output = None,
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

}

object SftpUploadActivity extends RunnableObject {

  def apply(host: String)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): SftpUploadActivity =
    new SftpUploadActivity(
      id = PipelineObjectId(SftpUploadActivity.getClass),
      scriptUri = Option(s"${hc.scriptUri}activities/run-jar.sh": HString),
      jarUri = s"${hc.scriptUri}activities/hyperion-sftp-activity-current-assembly.jar",
      mainClass = "com.krux.hyperion.contrib.activity.sftp.SftpActivity",
      host = host,
      port = None,
      username = None,
      password = None,
      identity = None,
      pattern = None,
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
