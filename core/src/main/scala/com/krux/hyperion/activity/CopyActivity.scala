package com.krux.hyperion.activity

import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpCopyActivity
import com.krux.hyperion.datanode.Copyable
import com.krux.hyperion.expression.DpPeriod
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{WorkerGroup, Ec2Resource}

/**
 * The activity that copies data from one data node to the other.
 *
 * @note it seems that both input and output format needs to be in CsvDataFormat for this copy to
 * work properly and it needs to be a specific variance of the CSV, for more information check the
 * web page:
 *
 * http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-copyactivity.html
 *
 * From our experience it's really hard to export using TsvDataFormat, in both import and export
 * especially for tasks involving RedshiftCopyActivity. A general rule of thumb is always use
 * default CsvDataFormat for tasks involving both exporting to S3 and copy to redshift.
 */
case class CopyActivity private (
  id: PipelineObjectId,
  input: Copyable,
  output: Copyable,
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

  override def objects: Iterable[PipelineObject] = runsOn.left.toSeq ++ Seq(input, output) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpCopyActivity(
    id = id,
    name = id.toOption,
    input = input.ref,
    output = output.ref,
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

object CopyActivity extends RunnableObject {

  def apply(input: Copyable, output: Copyable, runsOn: Ec2Resource): CopyActivity = apply(input, output, Left(runsOn))

  def apply(input: Copyable, output: Copyable, runsOn: WorkerGroup): CopyActivity = apply(input, output, Right(runsOn))

  private def apply(input: Copyable, output: Copyable, runsOn: Either[Ec2Resource, WorkerGroup]): CopyActivity =
    new CopyActivity(
      id = PipelineObjectId(CopyActivity.getClass),
      input = input,
      output = output,
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
