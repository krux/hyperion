package com.krux.hyperion.activity

import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{WorkerGroup, Ec2Resource}

/**
 * Shell command activity that runs a given Jar
 */
case class JarActivity private (
  id: PipelineObjectId,
  jar: String,
  scriptUri: Option[String],
  mainClass: Option[String],
  arguments: Seq[String],
  stdout: Option[String],
  stderr: Option[String],
  stage: Option[Boolean],
  input: Seq[S3DataNode],
  output: Seq[S3DataNode],
  runsOn: Either[Ec2Resource, WorkerGroup],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm],
  attemptTimeout: Option[String],
  lateAfterTimeout: Option[String],
  maximumRetries: Option[Int],
  retryDelay: Option[String],
  failureAndRerunMode: Option[FailureAndRerunMode]
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withMainClass(mainClass: Any): JarActivity = this.copy(mainClass = getMainClass(mainClass))
  def withArguments(args: String*) = this.copy(arguments = arguments ++ args)
  def withStdoutTo(out: String) = this.copy(stdout = Option(out))
  def withStderrTo(err: String) = this.copy(stderr = Option(err))
  def withInput(inputs: S3DataNode*) = this.copy(input = input ++ inputs, stage = Option(true))
  def withOutput(outputs: S3DataNode*) = this.copy(output = output ++ outputs, stage = Option(true))

  private[hyperion] def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)
  def withAttemptTimeout(timeout: String) = this.copy(attemptTimeout = Option(timeout))
  def withLateAfterTimeout(timeout: String) = this.copy(lateAfterTimeout = Option(timeout))
  def withMaximumRetries(retries: Int) = this.copy(maximumRetries = Option(retries))
  def withRetryDelay(delay: String) = this.copy(retryDelay = Option(delay))
  def withFailureAndRerunMode(mode: FailureAndRerunMode) = this.copy(failureAndRerunMode = Option(mode))

  override def objects: Iterable[PipelineObject] = runsOn.left.toSeq ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = scriptUri,
    scriptArgument = Option(Seq(jar) ++ mainClass.toSeq ++ arguments),
    stdout = stdout,
    stderr = stderr,
    stage = stage.map(_.toString),
    input = seqToOption(input)(_.ref),
    output = seqToOption(output)(_.ref),
    workerGroup = runsOn.right.toOption.map(_.ref),
    runsOn = runsOn.left.toOption.map(_.ref),
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref),
    attemptTimeout = attemptTimeout,
    lateAfterTimeout = lateAfterTimeout,
    maximumRetries = maximumRetries.map(_.toString),
    retryDelay = retryDelay,
    failureAndRerunMode = failureAndRerunMode.map(_.toString)
  )

}

object JarActivity extends RunnableObject {

  def apply(jar: String, runsOn: Ec2Resource)(implicit hc: HyperionContext): JarActivity = apply(jar, Left(runsOn))

  def apply(jar: String, runsOn: WorkerGroup)(implicit hc: HyperionContext): JarActivity = apply(jar, Right(runsOn))

  private def apply(jar: String, runsOn: Either[Ec2Resource, WorkerGroup])(implicit hc: HyperionContext): JarActivity =
    new JarActivity(
      id = PipelineObjectId(JarActivity.getClass),
      jar = jar,
      scriptUri = Option(s"${hc.scriptUri}activities/run-jar.sh"),
      mainClass = None,
      arguments = Seq(),
      stdout = None,
      stderr = None,
      stage = None,
      input = Seq(),
      output = Seq(),
      runsOn = runsOn,
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