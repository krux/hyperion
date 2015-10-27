package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpRedshiftCopyActivity
import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.datanode.{S3DataNode, RedshiftDataNode}
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.adt.{HInt, HDuration, HString}
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

/**
 * Copies data directly from DynamoDB or Amazon S3 to Amazon Redshift. You can load data into a new
 * table, or easily merge data into an existing table.
 */
case class RedshiftCopyActivity private (
  id: PipelineObjectId,
  insertMode: RedshiftCopyActivity.InsertMode,
  transformSql: Option[HString],
  queue: Option[HString],
  commandOptions: Seq[RedshiftCopyOption],
  input: S3DataNode,
  output: RedshiftDataNode,
  runsOn: Resource[Ec2Resource],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm],
  attemptTimeout: Option[HDuration],
  lateAfterTimeout: Option[HDuration],
  maximumRetries: Option[HInt],
  retryDelay: Option[HDuration],
  failureAndRerunMode: Option[FailureAndRerunMode]
) extends PipelineActivity {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withCommandOptions(opts: RedshiftCopyOption*) = this.copy(commandOptions = commandOptions ++ opts)
  def withTransformSql(sql: HString) = this.copy(transformSql = Option(sql))
  def withQueue(queue: HString) = this.copy(queue = Option(queue))

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

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ Seq(input, output) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpRedshiftCopyActivity(
    id = id,
    name = id.toOption,
    insertMode = insertMode.toString,
    transformSql = transformSql.map(_.serialize),
    queue = queue.map(_.serialize),
    commandOptions = seqToOption(commandOptions)(_.repr).map(_.flatten),
    input = input.ref,
    output = output.ref,
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

object RedshiftCopyActivity extends Enumeration with RunnableObject {

  type InsertMode = Value
  val KeepExisting = Value("KEEP_EXISTING")
  val OverwriteExisting = Value("OVERWRITE_EXISTING")
  val Truncate = Value("TRUNCATE")

  def apply(input: S3DataNode, output: RedshiftDataNode, insertMode: InsertMode)(runsOn: Resource[Ec2Resource]): RedshiftCopyActivity =
    new RedshiftCopyActivity(
      id = PipelineObjectId(RedshiftCopyActivity.getClass),
      insertMode = insertMode,
      transformSql = None,
      queue = None,
      commandOptions = Seq.empty,
      input = input,
      output = output,
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
