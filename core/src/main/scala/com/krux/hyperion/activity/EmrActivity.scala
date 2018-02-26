package com.krux.hyperion.activity

import com.krux.hyperion.adt.HString
import com.krux.hyperion.aws.AdpEmrActivity
import com.krux.hyperion.common.{BaseFields, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.resource.{BaseEmrCluster, Resource}


case class EmrActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[BaseEmrCluster],
  steps: Seq[BaseEmrStep],
  inputs: Seq[S3DataNode],
  outputs: Seq[S3DataNode],
  preStepCommands: Seq[HString],
  postStepCommands: Seq[HString]
) extends BaseEmrActivity[BaseEmrCluster] {

  type Self = EmrActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[BaseEmrCluster]) = copy(activityFields = fields)

  def withSteps(newSteps: BaseEmrStep*) = copy(steps = steps ++ newSteps)
  def withInput(input: S3DataNode*) = copy(inputs = inputs ++ input)
  def withOutput(output: S3DataNode*) = copy(outputs = outputs ++ output)

  def withPreStepCommand(commands: HString*): Self = copy(preStepCommands = preStepCommands ++ commands)
  def withPostStepCommand(commands: HString*): Self = copy(postStepCommands = postStepCommands ++ commands)

  override def objects = inputs ++ outputs ++ super.objects

  lazy val serialize = AdpEmrActivity(
    id = id,
    name = name,
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
    failureAndRerunMode = failureAndRerunMode.map(_.serialize),
    maxActiveInstances = maxActiveInstances.map(_.serialize)
  )

}

object EmrActivity extends RunnableObject {

  def apply(runsOn: Resource[BaseEmrCluster]): EmrActivity = new EmrActivity(
    baseFields = BaseFields(PipelineObjectId(EmrActivity.getClass)),
    activityFields = ActivityFields(runsOn),
    steps = Seq.empty,
    inputs = Seq.empty,
    outputs = Seq.empty,
    preStepCommands = Seq.empty,
    postStepCommands = Seq.empty
  )

}
