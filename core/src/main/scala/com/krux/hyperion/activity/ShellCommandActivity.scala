package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{S3Uri, PipelineObjectId, PipelineObject}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.DpPeriod
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{WorkerGroup, Ec2Resource}

/**
 * Runs a command or script
 */
case class ShellCommandActivity private (
  id: PipelineObjectId,
  script: Either[S3Uri, String],
  scriptArguments: Seq[String],
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
  attemptTimeout: Option[DpPeriod],
  lateAfterTimeout: Option[DpPeriod],
  maximumRetries: Option[Int],
  retryDelay: Option[DpPeriod],
  failureAndRerunMode: Option[FailureAndRerunMode]
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withArguments(args: String*) = this.copy(scriptArguments = scriptArguments ++ args)
  def withStdoutTo(out: String) = this.copy(stdout = Option(out))
  def withStderrTo(err: String) = this.copy(stderr = Option(err))
  def withInput(inputs: S3DataNode*) = this.copy(input = input ++ inputs, stage = Option(true))
  def withOutput(outputs: S3DataNode*) = this.copy(output = output ++ outputs, stage = Option(true))

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

  override def objects: Iterable[PipelineObject] = runsOn.left.toSeq ++ preconditions ++ input ++ output ++ dependsOn ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = script.right.toOption,
    scriptUri = script.left.toOption.map(_.ref),
    scriptArgument = scriptArguments,
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
    attemptTimeout = attemptTimeout.map(_.toString),
    lateAfterTimeout = lateAfterTimeout.map(_.toString),
    maximumRetries = maximumRetries.map(_.toString),
    retryDelay = retryDelay.map(_.toString),
    failureAndRerunMode = failureAndRerunMode.map(_.toString)
  )
}

object ShellCommandActivity extends RunnableObject {

  def apply(script: S3Uri, runsOn: Ec2Resource): ShellCommandActivity = apply(Left(script), Left(runsOn))

  def apply(script: String, runsOn: Ec2Resource): ShellCommandActivity = apply(Right(script), Left(runsOn))

  def apply(script: S3Uri, runsOn: WorkerGroup): ShellCommandActivity = apply(Left(script), Right(runsOn))

  def apply(script: String, runsOn: WorkerGroup): ShellCommandActivity = apply(Right(script), Right(runsOn))

  private def apply(script: Either[S3Uri, String], runsOn: Either[Ec2Resource, WorkerGroup]): ShellCommandActivity =
    new ShellCommandActivity(
      id = PipelineObjectId(ShellCommandActivity.getClass),
      script = script,
      scriptArguments = Seq(),
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
