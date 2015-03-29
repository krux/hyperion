package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpSqlActivity, AdpEc2Resource, AdpRef, AdpDatabase,
  AdpActivity, AdpSnsAlarm}

case class SqlActivity (
  id: String,
  runsOn: Ec2Resource,
  database: Database,
  script: String,
  scriptArgument: Seq[String],
  dependsOn: Seq[PipelineActivity],
  queue: Option[String] = None,
  onFailAlarms: Seq[SnsAlarm] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onLateActionAlarms: Seq[SnsAlarm] = Seq()
) extends PipelineActivity {

  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def withQueue(queue: String) = this.copy(queue = Option(queue))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)

  override def objects: Iterable[PipelineObject] =
    Seq(runsOn, database) ++ dependsOn ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpSqlActivity(
      id = id,
      name = Some(id),
      database = AdpRef[AdpDatabase](database.id),
      script = script,
      scriptArgument = scriptArgument match {
        case Seq() => None
        case other => Some(other)
      },
      queue = queue,
      dependsOn = dependsOn match {
        case Seq() => None
        case other => Some(other.map(a => AdpRef[AdpActivity](a.id)))
      },
      runsOn = AdpRef[AdpEc2Resource](runsOn.id),
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
