package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpJsonSerializer, AdpSqlActivity, AdpRef,
  AdpRedshiftDatabase, AdpEc2Resource, AdpActivity, AdpPrecondition}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpSnsAlarm

/**
 * Redshift unload activity
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
  onLateActionAlarms: Seq[SnsAlarm]
)(
  implicit val hc: HyperionContext
) extends PipelineActivity {

  def unloadScript = s"""
    UNLOAD ('${script.replaceAll("'", "\\\\\\\\'")}')
    TO '$s3Path'
    WITH CREDENTIALS AS
    'aws_access_key_id=${hc.accessKeyId};aws_secret_access_key=${hc.accessKeySecret}'
    ${unloadOptions.map(_.repr).flatten.mkString(" ")}
  """

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))

  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withUnloadOptions(opts: RedshiftUnloadOption*) = this.copy(unloadOptions = opts)

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(database, runsOn) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpSqlActivity(
    id = id,
    name = Some(id),
    database = AdpRef[AdpRedshiftDatabase](database.id),
    script = unloadScript,
    scriptArgument = None,
    queue = None,
    runsOn = AdpRef[AdpEc2Resource](runsOn.id),
    dependsOn = dependsOn match {
      case Seq() => None
      case deps => Some(deps.map(act => AdpRef[AdpActivity](act.id)))
    },
    precondition = preconditions match {
      case Seq() => None
      case preconditions => Some(preconditions.map(precondition => AdpRef[AdpPrecondition](precondition.id)))
    },
    onFail = onFailAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    },
    onSuccess = onSuccessAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    },
    onLateAction = onLateActionAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    }
  )

}

object RedshiftUnloadActivity {
  def apply(database: RedshiftDatabase, script: String, s3Path: String, runsOn: Ec2Resource)(implicit hc: HyperionContext) =
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
      onLateActionAlarms = Seq()
    )
}
