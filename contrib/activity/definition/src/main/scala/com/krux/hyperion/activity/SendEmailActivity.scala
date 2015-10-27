package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.{RunnableObject, Parameter}
import com.krux.hyperion.adt.{HInt, HDuration, HString, HBoolean, HType}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

class SendEmailActivity private (
  val id: PipelineObjectId,
  val scriptUri: Option[HString],
  val jarUri: HString,
  val mainClass: HString,
  val host: Option[HString],
  val port: Option[HInt],
  val username: Option[HString],
  val password: Option[Parameter[String]],
  val from: Option[HString],
  val to: Seq[HString],
  val cc: Seq[HString],
  val bcc: Seq[HString],
  val subject: Option[HString],
  val body: Option[HString],
  val starttls: HBoolean,
  val debug: HBoolean,
  val input: Seq[S3DataNode],
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
) extends PipelineActivity {

  require(password.forall(_.isEncrypted), "The password must be an encrypted string parameter")

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withHost(host: HString) = this.copy(host = Option(host))
  def withPort(port: HInt) = this.copy(port = Option(port))
  def withUsername(username: HString) = this.copy(username = Option(username))
  def withPassword(password: Parameter[String]) = this.copy(password = Option(password))
  def withFrom(from: HString) = this.copy(from = Option(from))
  def withTo(to: HString) = this.copy(to = this.to :+ to)
  def withCc(cc: HString) = this.copy(cc = this.cc :+ cc)
  def withBcc(bcc: HString) = this.copy(bcc = this.bcc :+ bcc)
  def withSubject(subject: HString) = this.copy(subject = Option(subject))
  def withBody(body: HString) = this.copy(body = Option(body))
  def withStartTls = this.copy(starttls = true)
  def withDebug = this.copy(debug = true)
  def withInput(inputs: S3DataNode*) = this.copy(input = input ++ inputs)
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
    host: Option[HString] = host,
    port: Option[HInt] = port,
    username: Option[HString] = username,
    password: Option[Parameter[String]] = password,
    from: Option[HString] = from,
    to: Seq[HString] = to,
    cc: Seq[HString] = cc,
    bcc: Seq[HString] = bcc,
    subject: Option[HString] = subject,
    body: Option[HString] = body,
    starttls: HBoolean = starttls,
    debug: HBoolean = debug,
    input: Seq[S3DataNode] = input,
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
  ) = new SendEmailActivity(id, scriptUri, jarUri, mainClass, host, port, username, password,
    from, to, cc, bcc, subject, body, starttls, debug, input, stdout, stderr, runsOn, dependsOn,
    preconditions, onFailAlarms, onSuccessAlarms, onLateActionAlarms, attemptTimeout, lateAfterTimeout,
    maximumRetries, retryDelay, failureAndRerunMode)

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ input ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  private def arguments: Seq[HType] = Seq(
    host.map(h => Seq[HString]("-H", h)),
    port.map(p => Seq[HType]("-P", p)),
    username.map(u => Seq[HString]("-u", u)),
    password.map(p => Seq[HType]("-p", p)),
    from.map(f => Seq[HString]("--from", f)),
    Option(to.flatMap(t => Seq[HString]("--to", t))),
    Option(cc.flatMap(c => Seq[HString]("--cc", c))),
    Option(bcc.flatMap(b => Seq[HString]("--bcc", b))),
    subject.map(s => Seq[HString]("-s", s)),
    body.map(b => Seq[HString]("-B", b)),
    if (starttls) Option(Seq[HString]("--starttls")) else None,
    if (debug) Option(Seq[HString]("--debug")) else None
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
    input = seqToOption(input)(_.ref),
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

object SendEmailActivity extends RunnableObject {

  def apply(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): SendEmailActivity =
    new SendEmailActivity(
      id = PipelineObjectId(SendEmailActivity.getClass),
      runsOn = runsOn,
      scriptUri = Option(s"${hc.scriptUri}activities/run-jar.sh": HString),
      jarUri = s"${hc.scriptUri}activities/hyperion-email-activity-current-assembly.jar",
      mainClass = "com.krux.hyperion.contrib.activity.email.SendEmailActivity",
      host = None,
      port = None,
      username = None,
      password = None,
      from = None,
      to = Seq.empty,
      cc = Seq.empty,
      bcc = Seq.empty,
      subject = None,
      body = None,
      starttls = false,
      debug = false,
      input = Seq.empty,
      stdout = None,
      stderr = None,
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
