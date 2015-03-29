package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpEmrActivity, AdpJsonSerializer, AdpRef, AdpEmrCluster,
  AdpActivity, AdpPrecondition}
import com.krux.hyperion.objects.aws.AdpSnsAlarm

/**
 * Defines a spark activity
 */
case class SparkActivity (
  id: String,
  runsOn: SparkCluster,
  steps: Seq[SparkStep] = Seq(),
  dependsOn: Seq[PipelineActivity] = Seq(),
  preconditions: Seq[Precondition] = Seq(),
  onFailAlarms: Seq[SnsAlarm] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onLateActionAlarms: Seq[SnsAlarm] = Seq()
) extends EmrActivity {

  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def withStepSeq(steps: Seq[SparkStep]) = this.copy(steps = steps)
  def withSteps(steps: SparkStep*) = this.copy(steps = steps)

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] =
    (runsOn +: dependsOn) ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpEmrActivity(
    id = id,
    name = Some(id),
    input = None,
    output = None,
    preStepCommand = None,
    postStepCommand = None,
    actionOnResourceFailure = None,
    actionOnTaskFailure = None,
    step = steps.map(_.toStepString),
    runsOn = AdpRef[AdpEmrCluster](runsOn.id),
    dependsOn = dependsOn match {
      case Seq() => None
      case deps => Some(deps.map(d => AdpRef[AdpActivity](d.id)))
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

object SparkActivity extends RunnableObject
