package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.AdpEmrActivity
import com.krux.hyperion.common.{ PipelineObjectId, BaseFields }
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.adt.HString
import com.krux.hyperion.resource.{ Resource, SparkCluster }

/**
 * Runs spark steps on given spark cluster with Amazon EMR
 */
case class SparkActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[SparkCluster],
  jobRunner: HString,
  scriptRunner: HString,
  steps: Seq[SparkStep],
  inputs: Seq[S3DataNode],
  outputs: Seq[S3DataNode],
  preStepCommands: Seq[HString],
  postStepCommands: Seq[HString]
)(implicit hc: HyperionContext) extends EmrActivity[SparkCluster] {

  type Self = SparkActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[SparkCluster]) = copy(activityFields = fields)

  def withSteps(step: SparkStep*) = {
    val transformed_step = step.map(_.withJobRunner(jobRunner)).map(_.withScriptRunner(scriptRunner))
    copy(steps = steps ++ transformed_step)
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

object SparkActivity extends RunnableObject {

  def jobRunner(runsOn: Resource[SparkCluster])(implicit hc: HyperionContext): HString = {
    if(runsOn.asManagedResource.exists(_.isReleaseLabel4xx)) {
      "spark-submit"
    } else {
      s"${hc.scriptUri}run-spark-step.sh"
    }
  }

  def scriptRunner(runsOn: Resource[SparkCluster]): HString = {
    if(runsOn.asManagedResource.exists(_.isReleaseLabel4xx)) {
      "command-runner.jar"
    } else {
      "s3://elasticmapreduce/libs/script-runner/script-runner.jar"
    }
  }

  def apply(runsOn: Resource[SparkCluster])(implicit hc: HyperionContext): SparkActivity = new SparkActivity(
    baseFields = BaseFields(PipelineObjectId(SparkActivity.getClass)),
    activityFields = ActivityFields(runsOn),
    jobRunner = jobRunner(runsOn)(hc),
    scriptRunner = scriptRunner(runsOn),
    steps = Seq.empty,
    inputs = Seq.empty,
    outputs = Seq.empty,
    preStepCommands = Seq.empty,
    postStepCommands = Seq.empty
  )

}
