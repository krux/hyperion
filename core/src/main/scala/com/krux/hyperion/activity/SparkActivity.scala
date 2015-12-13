package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpEmrActivity
import com.krux.hyperion.common.{ PipelineObjectId, PipelineObject, ObjectFields }
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.adt.{ HInt, HDuration, HString }
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{ Resource, SparkCluster }

/**
 * Runs spark steps on given spark cluster with Amazon EMR
 */
case class SparkActivity private (
  baseFields: ObjectFields,
  activityFields: ActivityFields[SparkCluster],
  steps: Seq[SparkStep],
  inputs: Seq[S3DataNode],
  outputs: Seq[S3DataNode],
  preStepCommands: Seq[HString],
  postStepCommands: Seq[HString]
) extends EmrActivity[SparkCluster] {

  type Self = SparkActivity

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[SparkCluster]) = copy(activityFields = fields)

  def withSteps(step: SparkStep*) = this.copy(steps = steps ++ step)
  def withPreStepCommand(command: HString*) = this.copy(preStepCommands = preStepCommands ++ command)
  def withPostStepCommand(command: HString*) = this.copy(postStepCommands = postStepCommands ++ command)
  def withInput(input: S3DataNode*) = this.copy(inputs = inputs ++ input)
  def withOutput(output: S3DataNode*) = this.copy(outputs = outputs ++ output)

  // def objects: Iterable[PipelineObject] = runsOn.toSeq ++ inputs ++ outputs ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpEmrActivity(
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

object SparkActivity extends RunnableObject {

  def apply(runsOn: Resource[SparkCluster]): SparkActivity = new SparkActivity(
    baseFields = ObjectFields(PipelineObjectId(SparkActivity.getClass)),
    activityFields = ActivityFields(runsOn),
    steps = Seq.empty,
    inputs = Seq.empty,
    outputs = Seq.empty,
    preStepCommands = Seq.empty,
    postStepCommands = Seq.empty
  )

}
