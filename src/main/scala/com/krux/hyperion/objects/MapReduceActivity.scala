package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpEmrActivity, AdpJsonSerializer, AdpRef, AdpEmrCluster,
  AdpActivity, AdpPrecondition}
import com.krux.hyperion.objects.aws.AdpSnsAlarm

/**
 * Defines a MapReduce activity
 */
case class MapReduceActivity private (
  id: PipelineObjectId,
  runsOn: EmrCluster,
  steps: Seq[MapReduceStep],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm]
) extends EmrActivity {

  def withStepSeq(steps: Seq[MapReduceStep]) = this.copy(steps = steps)
  def withSteps(steps: MapReduceStep*) = this.copy(steps = steps)

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))

  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

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
    dependsOn = seqToOption(dependsOn)(d => AdpRef[AdpActivity](d.id)),
    precondition = seqToOption(preconditions)(precondition => AdpRef[AdpPrecondition](precondition.id)),
    onFail = seqToOption(onFailAlarms)(alarm => AdpRef[AdpSnsAlarm](alarm.id)),
    onSuccess = seqToOption(onSuccessAlarms)(alarm => AdpRef[AdpSnsAlarm](alarm.id)),
    onLateAction = seqToOption(onLateActionAlarms)(alarm => AdpRef[AdpSnsAlarm](alarm.id))
  )

}

object MapReduceActivity extends RunnableObject {
  def apply(runsOn: EmrCluster) =
    new MapReduceActivity(
      id = PipelineObjectId("MapReduceActivity"),
      runsOn = runsOn,
      steps = Seq(),
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )
}
