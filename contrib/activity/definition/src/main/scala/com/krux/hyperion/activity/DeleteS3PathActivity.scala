package com.krux.hyperion.activity

import com.krux.hyperion.common.{S3Uri, PipelineObjectId, PipelineObject}
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.expression.Duration
import com.krux.hyperion.parameter.Parameter
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

/**
 * Activity to recursively delete files in an S3 path.
 */
case class DeleteS3PathActivity private (
  id: PipelineObjectId,
  s3Path: Parameter[S3Uri],
  stdout: Option[String],
  stderr: Option[String],
  runsOn: Resource[Ec2Resource],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm],
  attemptTimeout: Option[Parameter[Duration]],
  lateAfterTimeout: Option[Parameter[Duration]],
  maximumRetries: Option[Parameter[Int]],
  retryDelay: Option[Parameter[Duration]],
  failureAndRerunMode: Option[FailureAndRerunMode]
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

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

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = Option(s"aws s3 rm --recursive $s3Path"),
    scriptUri = None,
    scriptArgument = None,
    stdout = stdout,
    stderr = stderr,
    stage = Option("false"),
    input = None,
    output = None,
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

object DeleteS3PathActivity extends RunnableObject {
  def apply(s3Path: Parameter[S3Uri])(runsOn: Resource[Ec2Resource]): DeleteS3PathActivity =
    new DeleteS3PathActivity(
      id = PipelineObjectId(DeleteS3PathActivity.getClass),
      s3Path = s3Path,
      stdout = None,
      stderr = None,
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
