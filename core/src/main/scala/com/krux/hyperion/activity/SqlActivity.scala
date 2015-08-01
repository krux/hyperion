package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpSqlActivity
import com.krux.hyperion.common.{S3Uri, PipelineObjectId, PipelineObject}
import com.krux.hyperion.database.RedshiftDatabase
import com.krux.hyperion.expression.DpPeriod
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{WorkerGroup, Ec2Resource}

/**
 * Runs an SQL query on a RedShift cluster. If the query writes out to a table that does not exist,
 * a new table with that name is created.
 */
case class SqlActivity private (
  id: PipelineObjectId,
  script: Either[S3Uri, String],
  scriptArgument: Seq[String],
  database: RedshiftDatabase,
  queue: Option[String],
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

  def withArguments(arg: String*) = this.copy(scriptArgument = scriptArgument ++ arg)
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

  override def objects: Iterable[PipelineObject] =
    runsOn.left.toSeq ++ Seq(database) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpSqlActivity(
    id = id,
    name = id.toOption,
    script = script.right.toOption,
    scriptUri = script.left.toOption.map(_.toString),
    scriptArgument = scriptArgument,
    database = database.ref,
    queue = queue,
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

object SqlActivity extends RunnableObject {
  def apply(database: RedshiftDatabase, script: S3Uri, runsOn: Ec2Resource): SqlActivity =
    apply(database, Left(script), Left(runsOn))

  def apply(database: RedshiftDatabase, script: String, runsOn: Ec2Resource): SqlActivity =
    apply(database, Right(script), Left(runsOn))

  def apply(database: RedshiftDatabase, script: S3Uri, runsOn: WorkerGroup): SqlActivity =
    apply(database, Left(script), Right(runsOn))

  def apply(database: RedshiftDatabase, script: String, runsOn: WorkerGroup): SqlActivity =
    apply(database, Right(script), Right(runsOn))

  private def apply(database: RedshiftDatabase, script: Either[S3Uri, String], runsOn: Either[Ec2Resource, WorkerGroup]): SqlActivity =
    new SqlActivity(
      id = PipelineObjectId(SqlActivity.getClass),
      script = script,
      scriptArgument = Seq(),
      database = database,
      queue = None,
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
