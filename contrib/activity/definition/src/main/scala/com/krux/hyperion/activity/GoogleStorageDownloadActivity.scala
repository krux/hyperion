package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{WorkerGroup, Ec2Resource}

/**
 * Google Storage Download activity
 */
case class GoogleStorageDownloadActivity private (
  id: PipelineObjectId,
  scriptUri: String,
  input: String,
  output: Option[S3DataNode],
  botoConfigUrl: String,
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
) extends GoogleStorageActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withInput(path: String) = this.copy(input = path)
  def withOutput(out: S3DataNode) = this.copy(output = Option(out))
  def withBotoConfigUrl(url: String) = this.copy(botoConfigUrl = url)

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

  override def objects: Iterable[PipelineObject] = runsOn.left.toSeq ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = Option(scriptUri),
    scriptArgument = Option(Seq(botoConfigUrl, input)),
    stdout = None,
    stderr = None,
    stage = Option("true"),
    input = None,
    output = output.map(out => Seq(out.ref)),
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

object GoogleStorageDownloadActivity {
  def apply(runsOn: Ec2Resource)(implicit hc: HyperionContext): GoogleStorageDownloadActivity =
    apply(Left(runsOn))

  def apply(runsOn: WorkerGroup)(implicit hc: HyperionContext): GoogleStorageDownloadActivity =
    apply(Right(runsOn))

  private def apply(runsOn: Either[Ec2Resource, WorkerGroup])(implicit hc: HyperionContext): GoogleStorageDownloadActivity =
    new GoogleStorageDownloadActivity(
      id = PipelineObjectId(GoogleStorageDownloadActivity.getClass),
      scriptUri = s"${hc.scriptUri}activities/gsutil-download.sh",
      input = "",
      output = None,
      botoConfigUrl = "",
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
