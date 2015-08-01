package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpRedshiftCopyActivity
import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.datanode.{S3DataNode, RedshiftDataNode}
import com.krux.hyperion.expression.DpPeriod
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{WorkerGroup, Ec2Resource}

/**
 * Copies data directly from DynamoDB or Amazon S3 to Amazon Redshift. You can load data into a new
 * table, or easily merge data into an existing table.
 */
case class RedshiftCopyActivity private (
  id: PipelineObjectId,
  insertMode: RedshiftCopyActivity.InsertMode,
  transformSql: Option[String],
  queue: Option[String],
  commandOptions: Seq[RedshiftCopyOption],
  input: S3DataNode,
  output: RedshiftDataNode,
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

  def withCommandOptions(opts: RedshiftCopyOption*) = this.copy(commandOptions = commandOptions ++ opts)
  def withTransformSql(sql: String) = this.copy(transformSql = Option(sql))
  def withQueue(queue: String) = this.copy(queue = Option(queue))

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

  lazy val serialize = AdpRedshiftCopyActivity(
    id = id,
    name = id.toOption,
    insertMode = insertMode.toString,
    transformSql = transformSql,
    queue = queue,
    commandOptions = seqToOption(commandOptions)(_.repr).map(_.flatten),
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

object RedshiftCopyActivity extends Enumeration with RunnableObject {

  type InsertMode = Value
  val KeepExisting = Value("KEEP_EXISTING")
  val OverwriteExisting = Value("OVERWRITE_EXISTING")
  val Truncate = Value("TRUNCATE")

  def apply(input: S3DataNode, output: RedshiftDataNode, insertMode: InsertMode,
    runsOn: Ec2Resource)(implicit hc: HyperionContext): RedshiftCopyActivity = apply(input, output, insertMode, Left(runsOn))

  def apply(input: S3DataNode, output: RedshiftDataNode, insertMode: InsertMode,
    runsOn: WorkerGroup)(implicit hc: HyperionContext): RedshiftCopyActivity = apply(input, output, insertMode, Right(runsOn))

  private def apply(input: S3DataNode, output: RedshiftDataNode, insertMode: InsertMode,
    runsOn: Either[Ec2Resource, WorkerGroup]): RedshiftCopyActivity =
    new RedshiftCopyActivity(
      id = PipelineObjectId(RedshiftCopyActivity.getClass),
      insertMode = insertMode,
      transformSql = None,
      queue = None,
      commandOptions = Seq(),
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
