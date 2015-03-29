package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpJsonSerializer, AdpSqlActivity, AdpRef,
  AdpRedshiftDatabase, AdpEc2Resource, AdpActivity}
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
    onFailAlarms: Seq[SnsAlarm] = Seq(),
    onSuccessAlarms: Seq[SnsAlarm] = Seq(),
    onLateActionAlarms: Seq[SnsAlarm] = Seq()
  )(
    implicit val hc: HyperionContext
  ) extends PipelineActivity {

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)

  def unloadScript = s"""
    UNLOAD ('${script.replaceAll("'", "\\\\\\\\'")}')
    TO '$s3Path'
    WITH CREDENTIALS AS
    'aws_access_key_id=${hc.accessKeyId};aws_secret_access_key=${hc.accessKeySecret}'
    ${unloadOptions.map(_.repr).flatten.mkString(" ")}
  """

  def withUnloadOptions(opts: RedshiftUnloadOption*) = this.copy(unloadOptions = opts)

  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(database, runsOn) ++ dependsOn ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpSqlActivity(
      id = id,
      name = Some(id),
      database = AdpRef[AdpRedshiftDatabase](database.id),
      script = unloadScript,
      scriptArgument = None,
      queue = None,
      dependsOn = dependsOn match {
        case Seq() => None
        case deps => Some(deps.map(act => AdpRef[AdpActivity](act.id)))
      },
      runsOn = AdpRef[AdpEc2Resource](runsOn.id),
      onFailAlarms match {
        case Seq() => None
        case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
      },
      onSuccessAlarms match {
        case Seq() => None
        case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
      },
      onLateActionAlarms match {
        case Seq() => None
        case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
      }
    )

}
