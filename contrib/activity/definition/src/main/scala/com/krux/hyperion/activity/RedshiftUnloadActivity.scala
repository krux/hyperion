package com.krux.hyperion.activity

import com.krux.hyperion.common.{S3Uri, PipelineObjectId, PipelineObject}
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpSqlActivity
import com.krux.hyperion.database.RedshiftDatabase
import com.krux.hyperion.expression.Duration
import com.krux.hyperion.parameter.{Parameter, StringParameter}
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

/**
 * Unload result of the given sql script from redshift to given s3Path.
 */
case class RedshiftUnloadActivity private (
  id: PipelineObjectId,
  script: String,
  s3Path: Parameter[S3Uri],
  database: RedshiftDatabase,
  unloadOptions: Seq[RedshiftUnloadOption],
  queue: Option[String],
  runsOn: Resource[Ec2Resource],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm],
  accessKeyId: StringParameter,
  accessKeySecret: StringParameter,
  attemptTimeout: Option[Parameter[Duration]],
  lateAfterTimeout: Option[Parameter[Duration]],
  maximumRetries: Option[Parameter[Int]],
  retryDelay: Option[Parameter[Duration]],
  failureAndRerunMode: Option[FailureAndRerunMode]
) extends PipelineActivity {

  require(accessKeyId.isEncrypted, "The access key id must be an encrypted string parameter")
  require(accessKeySecret.isEncrypted, "The access secret must be an encrypted string parameter")

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
  def withAttemptTimeout(timeout: Parameter[Duration]) = this.copy(attemptTimeout = Option(timeout))
  def withLateAfterTimeout(timeout: Parameter[Duration]) = this.copy(lateAfterTimeout = Option(timeout))
  def withMaximumRetries(retries: Parameter[Int]) = this.copy(maximumRetries = Option(retries))
  def withRetryDelay(delay: Parameter[Duration]) = this.copy(retryDelay = Option(delay))
  def withFailureAndRerunMode(mode: FailureAndRerunMode) = this.copy(failureAndRerunMode = Option(mode))

  def objects: Iterable[PipelineObject] =
    runsOn.toSeq ++
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

object RedshiftUnloadActivity extends RunnableObject {

  def apply(database: RedshiftDatabase, script: String, s3Path: Parameter[S3Uri],
    accessKeyId: StringParameter, accessKeySecret: StringParameter)(runsOn: Resource[Ec2Resource]): RedshiftUnloadActivity =
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
