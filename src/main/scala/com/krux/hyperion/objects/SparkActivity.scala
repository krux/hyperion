package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpEmrActivity, AdpJsonSerializer, AdpRef, AdpEmrCluster,
  AdpActivity}
import com.krux.hyperion.objects.aws.AdpSnsAlarm

/**
 * Defines a spark activity
 */
case class SparkActivity (
    id: String,
    runsOn: SparkCluster,
    steps: Seq[SparkStep] = Seq(),
    dependsOn: Seq[PipelineActivity] = Seq(),
    onFailAlarms: Seq[SnsAlarm] = Seq(),
    onSuccessAlarms: Seq[SnsAlarm] = Seq(),
    onLateActionAlarms: Seq[SnsAlarm] = Seq()
  ) extends EmrActivity {

  def withStepSeq(steps: Seq[SparkStep]) = this.copy(steps = steps)
  def withSteps(steps: SparkStep*) = this.copy(steps = steps)

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] =
    (runsOn +: dependsOn) ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpEmrActivity(
      id,
      Some(id),
      None,
      None,
      None,
      None,
      None,
      None,
      steps.map(_.toStepString),
      AdpRef[AdpEmrCluster](runsOn.id),
      dependsOn match {
        case Seq() => None
        case deps => Some(deps.map(d => AdpRef[AdpActivity](d.id)))
      },
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

object SparkActivity extends RunnableObject
