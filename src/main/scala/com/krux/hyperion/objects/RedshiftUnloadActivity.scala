package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpJsonSerializer, AdpSqlActivity, AdpRef,
  AdpRedshiftDatabase, AdpEc2Resource, AdpActivity, AdpPrecondition}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpSnsAlarm

/**
 * Redshift unload activity
 */
case class RedshiftUnloadActivity(
  id: String,
  database: RedshiftDatabase,
  script: String,
  s3Path: String,
  runsOn: Ec2Resource,
  unloadOptions: Seq[RedshiftUnloadOption] = Seq(),
  dependsOn: Seq[PipelineActivity] = Seq(),
  preconditions: Seq[Precondition] = Seq(),
  onFailAlarms: Seq[SnsAlarm] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onLateActionAlarms: Seq[SnsAlarm] = Seq()
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
