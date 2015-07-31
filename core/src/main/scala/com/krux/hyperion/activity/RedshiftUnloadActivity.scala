package com.krux.hyperion.activity

import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpSqlActivity
import com.krux.hyperion.database.RedshiftDatabase
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{WorkerGroup, Ec2Resource}

/**
 * Unload result of the given sql script from redshift to given s3Path.
 */
case class RedshiftUnloadActivity private (
  id: PipelineObjectId,
  script: String,
  s3Path: String,
  database: RedshiftDatabase,
  unloadOptions: Seq[RedshiftUnloadOption],
  queue: Option[String],
  runsOn: Either[Ec2Resource, WorkerGroup],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm],
  accessKeyId: String,
  accessKeySecret: String,
  attemptTimeout: Option[String],
  lateAfterTimeout: Option[String],
  maximumRetries: Option[Int],
  retryDelay: Option[String],
  failureAndRerunMode: Option[FailureAndRerunMode]
) extends PipelineActivity {

  def unloadScript = s"""
    UNLOAD ('${script.replaceAll("'", "\\\\\\\\'")}')
    TO '$s3Path'
    WITH CREDENTIALS AS
    'aws_access_key_id=$accessKeyId;aws_secret_access_key=$accessKeySecret'
    ${unloadOptions.flatMap(_.repr).mkString(" ")}
  """

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withUnloadOptions(opts: RedshiftUnloadOption*) =
    this.copy(unloadOptions = unloadOptions ++ opts)

  def withQueue(queue: String) = this.copy(queue = Option(queue))

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

  override def objects: Iterable[PipelineObject] =
    runsOn.left.toSeq ++
    Seq(database) ++
    dependsOn ++
    preconditions ++
    onFailAlarms ++
    onSuccessAlarms ++
    onLateActionAlarms

  lazy val serialize = AdpSqlActivity(
    id = id,
    name = id.toOption,
    script = Option(unloadScript),
    scriptUri = None,
    scriptArgument = None,
    database = database.ref,
    queue = queue,
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

object RedshiftUnloadActivity extends RunnableObject {

  def apply(database: RedshiftDatabase, script: String, s3Path: String,
    accessKeyId: String, accessKeySecret: String, runsOn: Ec2Resource): RedshiftUnloadActivity =
    apply(database, script, s3Path, accessKeyId, accessKeySecret, Left(runsOn))

  def apply(database: RedshiftDatabase, script: String, s3Path: String,
    accessKeyId: String, accessKeySecret: String, runsOn: WorkerGroup): RedshiftUnloadActivity =
    apply(database, script, s3Path, accessKeyId, accessKeySecret, Right(runsOn))

  private def apply(database: RedshiftDatabase, script: String, s3Path: String,
    accessKeyId: String, accessKeySecret: String, runsOn: Either[Ec2Resource, WorkerGroup]): RedshiftUnloadActivity =
    new RedshiftUnloadActivity(
      id = PipelineObjectId(RedshiftUnloadActivity.getClass),
      script = script,
      s3Path = s3Path,
      database = database,
      queue = None,
      runsOn = runsOn,
      unloadOptions = Seq(),
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq(),
      accessKeyId = accessKeyId,
      accessKeySecret = accessKeySecret,
      attemptTimeout = None,
      lateAfterTimeout = None,
      maximumRetries = None,
      retryDelay = None,
      failureAndRerunMode = None
    )

}
