package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpSqlActivity, AdpEc2Resource, AdpRef, AdpDatabase,
  AdpActivity, AdpSnsAlarm, AdpPrecondition}

case class SqlActivity (
  id: String,
  runsOn: Ec2Resource,
  database: Database,
  script: String,
  scriptArgument: Seq[String] = Seq(),
  queue: Option[String] = None,
  dependsOn: Seq[PipelineActivity] = Seq(),
  preconditions: Seq[Precondition] = Seq(),
  onFailAlarms: Seq[SnsAlarm] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onLateActionAlarms: Seq[SnsAlarm] = Seq()
) extends PipelineActivity {

  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def withQueue(queue: String) = this.copy(queue = Option(queue))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] =
    Seq(runsOn, database) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

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
    runsOn = AdpRef[AdpEc2Resource](runsOn.id),
    dependsOn = dependsOn match {
      case Seq() => None
      case other => Some(other.map(a => AdpRef[AdpActivity](a.id)))
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
