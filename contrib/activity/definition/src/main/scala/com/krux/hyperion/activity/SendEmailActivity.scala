package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.DpPeriod
import com.krux.hyperion.parameter.StringParameter
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{WorkerGroup, Ec2Resource}

class SendEmailActivity private (
  val id: PipelineObjectId,
  val scriptUri: Option[String],
  val jarUri: String,
  val mainClass: String,
  val host: Option[String],
  val port: Option[Int],
  val username: Option[String],
  val password: Option[StringParameter],
  val from: Option[String],
  val to: Seq[String],
  val cc: Seq[String],
  val bcc: Seq[String],
  val subject: Option[String],
  val body: Option[String],
  val starttls: Boolean,
  val debug: Boolean,
  val input: Seq[S3DataNode],
  val stdout: Option[String],
  val stderr: Option[String],
  val runsOn: Either[Ec2Resource, WorkerGroup],
  val dependsOn: Seq[PipelineActivity],
  val preconditions: Seq[Precondition],
  val onFailAlarms: Seq[SnsAlarm],
  val onSuccessAlarms: Seq[SnsAlarm],
  val onLateActionAlarms: Seq[SnsAlarm],
  val attemptTimeout: Option[DpPeriod],
  val lateAfterTimeout: Option[DpPeriod],
  val maximumRetries: Option[Int],
  val retryDelay: Option[DpPeriod],
  val failureAndRerunMode: Option[FailureAndRerunMode]
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withHost(host: String) = this.copy(host = Option(host))
  def withPort(port: Int) = this.copy(port = Option(port))
  def withUsername(username: String) = this.copy(username = Option(username))
  def withPassword(password: StringParameter) = this.copy(password = Option(password))
  def withFrom(from: String) = this.copy(from = Option(from))
  def withTo(to: String) = this.copy(to = this.to :+ to)
  def withCc(cc: String) = this.copy(cc = this.cc :+ cc)
  def withBcc(bcc: String) = this.copy(bcc = this.bcc :+ bcc)
  def withSubject(subject: String) = this.copy(subject = Option(subject))
  def withBody(body: String) = this.copy(body = Option(body))
  def withInput(inputs: S3DataNode*) = this.copy(input = input ++ inputs)
  def withStdoutTo(out: String) = this.copy(stdout = Option(out))
  def withStderrTo(err: String) = this.copy(stderr = Option(err))

  private[hyperion] def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)
  def withAttemptTimeout(timeout: DpPeriod) = this.copy(attemptTimeout = Option(timeout))
  def withLateAfterTimeout(timeout: DpPeriod) = this.copy(lateAfterTimeout = Option(timeout))
  def withMaximumRetries(retries: Int) = this.copy(maximumRetries = Option(retries))
  def withRetryDelay(delay: DpPeriod) = this.copy(retryDelay = Option(delay))
  def withFailureAndRerunMode(mode: FailureAndRerunMode) = this.copy(failureAndRerunMode = Option(mode))

  def copy(
    id: PipelineObjectId = id,
    scriptUri: Option[String] = scriptUri,
    jarUri: String = jarUri,
    mainClass: String = mainClass,
    host: Option[String] = host,
    port: Option[Int] = port,
    username: Option[String] = username,
    password: Option[StringParameter] = password,
    from: Option[String] = from,
    to: Seq[String] = to,
    cc: Seq[String] = cc,
    bcc: Seq[String] = bcc,
    subject: Option[String] = subject,
    body: Option[String] = body,
    starttls: Boolean = starttls,
    debug: Boolean = debug,
    input: Seq[S3DataNode] = input,
    stdout: Option[String] = stdout,
    stderr: Option[String] = stderr,
    runsOn: Either[Ec2Resource, WorkerGroup] = runsOn,
    dependsOn: Seq[PipelineActivity] = dependsOn,
    preconditions: Seq[Precondition] = preconditions,
    onFailAlarms: Seq[SnsAlarm] = onFailAlarms,
    onSuccessAlarms: Seq[SnsAlarm] = onSuccessAlarms,
    onLateActionAlarms: Seq[SnsAlarm] = onLateActionAlarms,
    attemptTimeout: Option[DpPeriod] = attemptTimeout,
    lateAfterTimeout: Option[DpPeriod] = lateAfterTimeout,
    maximumRetries: Option[Int] = maximumRetries,
    retryDelay: Option[DpPeriod] = retryDelay,
    failureAndRerunMode: Option[FailureAndRerunMode] = failureAndRerunMode
  ) = new SendEmailActivity(id, scriptUri, jarUri, mainClass, host, port, username, password,
    from, to, cc, bcc, subject, body, starttls, debug, input, stdout, stderr, runsOn, dependsOn,
    preconditions, onFailAlarms, onSuccessAlarms, onLateActionAlarms, attemptTimeout, lateAfterTimeout,
    maximumRetries, retryDelay, failureAndRerunMode)

  override def objects: Iterable[PipelineObject] = runsOn.left.toSeq ++ input ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  private def arguments: Seq[String] = Seq(
    host.map(h => Seq("-H", h)),
    port.map(p => Seq("-P", p.toString)),
    username.map(u => Seq("-u", u)),
    password.map(p => Seq("-p", p.toString)),
    from.map(f => Seq("--from", f)),
    Option(to.flatMap(t => Seq("--to", t))),
    Option(cc.flatMap(c => Seq("--cc", c))),
    Option(bcc.flatMap(b => Seq("--bcc", b))),
    subject.map(s => Seq("-s", s)),
    body.map(b => Seq("-B", b)),
    if (starttls) Option(Seq("--starttls")) else None,
    if (debug) Option(Seq("--debug")) else None
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
    input = seqToOption(input)(_.ref),
    output = None,
    workerGroup = runsOn.right.toOption.map(_.ref),
    runsOn = runsOn.left.toOption.map(_.ref),
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

object SendEmailActivity {

  def apply(runsOn: Ec2Resource)(implicit hc: HyperionContext): SendEmailActivity = apply(Left(runsOn))

  def apply(runsOn: WorkerGroup)(implicit hc: HyperionContext): SendEmailActivity = apply(Right(runsOn))

  private def apply(runsOn: Either[Ec2Resource, WorkerGroup])(implicit hc: HyperionContext): SendEmailActivity =
    new SendEmailActivity(
      id = PipelineObjectId(SendEmailActivity.getClass),
      runsOn = runsOn,
      scriptUri = Option(s"${hc.scriptUri}activities/run-jar.sh"),
      jarUri = s"${hc.scriptUri}activities/hyperion-contrib-activity-email-assembly-current.jar",
      mainClass = "com.krux.hyperion.contrib.activity.email.SendEmailActivity",
      host = None,
      port = None,
      username = None,
      password = None,
      from = None,
      to = Seq(),
      cc = Seq(),
      bcc = Seq(),
      subject = None,
      body = None,
      starttls = false,
      debug = false,
      input = Seq(),
      stdout = None,
      stderr = None,
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq(),
      attemptTimeout = None,
      lateAfterTimeout = None,
      maximumRetries = None,
      retryDelay = None,
      failureAndRerunMode = None
    )

}
