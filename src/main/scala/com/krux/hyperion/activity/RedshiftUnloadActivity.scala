package com.krux.hyperion.activity

import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpSqlActivity
import com.krux.hyperion.database.RedshiftDatabase
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.Ec2Resource

/**
 * Unload result of the given sql script from redshift to given s3Path.
 */
case class RedshiftUnloadActivity private (
  id: PipelineObjectId,
  database: RedshiftDatabase,
  script: String,
  s3Path: String,
  runsOn: Ec2Resource,
  unloadOptions: Seq[RedshiftUnloadOption],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm],
  accessKeyId: String,
  accessKeySecret: String
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

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)

  override def objects: Iterable[PipelineObject] =
    Seq(database, runsOn) ++
    dependsOn ++
    preconditions ++
    onFailAlarms ++
    onSuccessAlarms ++
    onLateActionAlarms

  lazy val serialize = AdpSqlActivity(
    id = id,
    name = id.toOption,
    database = database.ref,
    script = unloadScript,
    scriptArgument = None,
    queue = None,
    runsOn = runsOn.ref,
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref)
  )

}

object RedshiftUnloadActivity extends RunnableObject {

  def apply(database: RedshiftDatabase, script: String, s3Path: String,
    accessKeyId: String, accessKeySecret: String, runsOn: Ec2Resource)(implicit hc: HyperionContext) =
    new RedshiftUnloadActivity(
      id = PipelineObjectId("RedshiftUnloadActivity"),
      database = database,
      script = script,
      s3Path = s3Path,
      runsOn = runsOn,
      unloadOptions = Seq(),
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq(),
      accessKeyId = accessKeyId,
      accessKeySecret = accessKeySecret
    )

}
