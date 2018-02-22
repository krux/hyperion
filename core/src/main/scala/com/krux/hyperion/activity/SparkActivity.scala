package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.AdpEmrActivity
import com.krux.hyperion.common.{SparkCommandRunner, PipelineObjectId, BaseFields}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.adt.HString
import com.krux.hyperion.resource.{Resource, LegacySparkCluster}


/**
 * Runs spark steps on given spark cluster with Amazon EMR
 */
case class LegacySparkActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[LegacySparkCluster],
  jobRunner: HString,
  scriptRunner: HString,
  steps: Seq[LegacySparkStep],
  inputs: Seq[S3DataNode],
  outputs: Seq[S3DataNode],
  preStepCommands: Seq[HString],
  postStepCommands: Seq[HString]
) extends EmrActivity[LegacySparkCluster] {

  type Self = LegacySparkActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[LegacySparkCluster]) = copy(activityFields = fields)

  def withSteps(moreSteps: LegacySparkStep*) = {
    val newSteps = moreSteps.map(step =>
      step.copy(
        scriptRunner = step.scriptRunner.orElse(Option(scriptRunner)),
        jobRunner = step.jobRunner.orElse(Option(jobRunner))
      )
    )
    copy(steps = steps ++ newSteps)
  }
  def withPreStepCommand(command: HString*) = copy(preStepCommands = preStepCommands ++ command)
  def withPostStepCommand(command: HString*) = copy(postStepCommands = postStepCommands ++ command)
  def withInput(input: S3DataNode*) = copy(inputs = inputs ++ input)
  def withOutput(output: S3DataNode*) = copy(outputs = outputs ++ output)

  override def objects = inputs ++ inputs ++ super.objects

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

object LegacySparkActivity extends RunnableObject with SparkCommandRunner {

  def apply(runsOn: Resource[LegacySparkCluster])(implicit hc: HyperionContext): LegacySparkActivity = new LegacySparkActivity(
    baseFields = BaseFields(PipelineObjectId(LegacySparkActivity.getClass)),
    activityFields = ActivityFields(runsOn),
    jobRunner = jobRunner(runsOn),
    scriptRunner = scriptRunner(runsOn),
    steps = Seq.empty,
    inputs = Seq.empty,
    outputs = Seq.empty,
    preStepCommands = Seq.empty,
    postStepCommands = Seq.empty
  )

}
