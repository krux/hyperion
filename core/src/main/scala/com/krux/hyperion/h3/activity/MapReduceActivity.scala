package com.krux.hyperion.h3.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.activity.MapReduceStep
import com.krux.hyperion.adt.{HInt, HDuration, HString}
import com.krux.hyperion.aws.AdpEmrActivity
import com.krux.hyperion.h3.common.PipelineObjectId
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource._
import com.krux.hyperion.h3.common.ObjectFields

/**
 * Runs map reduce steps on an Amazon EMR cluster
 */
case class MapReduceActivity[A <: EmrCluster] private (
  steps: Seq[MapReduceStep],
  baseFields: ObjectFields,
  activityFields: ActivityFields[A],
  emrActivityFields: EmrActivityFields,
  inputs: Seq[S3DataNode],
  outputs: Seq[S3DataNode]
) extends EmrActivity[A] {

  type Self = MapReduceActivity[A]

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[A]) = copy(activityFields = fields)
  def updateEmrActivityFields(fields: EmrActivityFields) = copy(emrActivityFields = fields)

  def withSteps(step: MapReduceStep*) = this.copy(steps = steps ++ step)
  def withInput(input: S3DataNode*) = this.copy(inputs = inputs ++ input)
  def withOutput(output: S3DataNode*) = this.copy(outputs = outputs ++ output)

  // def objects: Iterable[PipelineObject] = runsOn.toSeq ++ inputs ++ outputs ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpEmrActivity(
    id = id,
    name = id.toOption,
    step = steps.map(_.serialize),
    preStepCommand = seqToOption(preStepCommands)(_.serialize),
    postStepCommand = seqToOption(postStepCommands)(_.serialize),
    input = seqToOption(inputs)(_.ref),
    output = seqToOption(outputs)(_.ref),
    workerGroup = runsOn.asWorkerGroup.map(_.ref),
    runsOn = runsOn.asManagedResource.map(_.ref),
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref),
    attemptTimeout = attemptTimeout.map(_.serialize),
    lateAfterTimeout = lateAfterTimeout.map(_.serialize),
    maximumRetries = maximumRetries.map(_.serialize),
    retryDelay = retryDelay.map(_.serialize),
    failureAndRerunMode = failureAndRerunMode.map(_.serialize)
  )

}

object MapReduceActivity extends RunnableObject {

  def apply[A <: EmrCluster](runsOn: Resource[A]): MapReduceActivity[A] =
    new MapReduceActivity(
      baseFields = ObjectFields(PipelineObjectId(MapReduceActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      emrActivityFields = EmrActivityFields(),
      steps = Seq.empty,
      inputs = Seq.empty,
      outputs = Seq.empty
    )

}
