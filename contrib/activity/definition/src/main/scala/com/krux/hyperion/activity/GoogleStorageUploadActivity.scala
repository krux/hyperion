package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{S3Uri, PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.DpPeriod
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{WorkerGroup, Ec2Resource}

/**
 * Google Storage Upload activity
 */
case class GoogleStorageUploadActivity private (
  id: PipelineObjectId,
  scriptUri: String,
  input: Option[S3DataNode],
  output: String,
  botoConfigUrl: S3Uri,
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
) extends GoogleStorageActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withInput(in: S3DataNode) = this.copy(input = Option(in))
  def withOutput(path: String) = this.copy(output = path)

  private[hyperion] def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions ++ preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)
  def withAttemptTimeout(timeout: DpPeriod) = this.copy(attemptTimeout = Option(timeout))
  def withLateAfterTimeout(timeout: DpPeriod) = this.copy(lateAfterTimeout = Option(timeout))
  def withMaximumRetries(retries: Int) = this.copy(maximumRetries = Option(retries))
  def withRetryDelay(delay: DpPeriod) = this.copy(retryDelay = Option(delay))
  def withFailureAndRerunMode(mode: FailureAndRerunMode) = this.copy(failureAndRerunMode = Option(mode))

  override def objects: Iterable[PipelineObject] = runsOn.left.toSeq ++ input ++ dependsOn

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = Option(scriptUri),
    scriptArgument = Option(Seq(botoConfigUrl.ref, output)),
    stdout = None,
    stderr = None,
    stage = Option("true"),
    input = input.map(in => Seq(in.ref)),
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

object GoogleStorageUploadActivity extends RunnableObject {
  def apply(botoConfigUrl: S3Uri, runsOn: Ec2Resource)(implicit hc: HyperionContext): GoogleStorageUploadActivity = apply(botoConfigUrl, Left(runsOn))

  def apply(botoConfigUrl: S3Uri, runsOn: WorkerGroup)(implicit hc: HyperionContext): GoogleStorageUploadActivity = apply(botoConfigUrl, Right(runsOn))

  private def apply(botoConfigUrl: S3Uri, runsOn: Either[Ec2Resource, WorkerGroup])(implicit hc: HyperionContext): GoogleStorageUploadActivity =
    new GoogleStorageUploadActivity(
      id = PipelineObjectId(GoogleStorageUploadActivity.getClass),
      scriptUri = s"${hc.scriptUri}activities/gsutil-upload.sh",
      input = None,
      output = "",
      botoConfigUrl = botoConfigUrl,
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
