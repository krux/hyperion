package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpEmrActivity
import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.adt.{HInt, HDuration, HString}
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource._

/**
 * Runs spark steps on given spark cluster with Amazon EMR
 */
case class SparkActivity private (
  id: PipelineObjectId,
  steps: Seq[SparkStep],
  preStepCommands: Seq[HString],
  postStepCommands: Seq[HString],
  inputs: Seq[S3DataNode],
  outputs: Seq[S3DataNode],
  runsOn: Resource[SparkCluster],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm],
  attemptTimeout: Option[HDuration],
  lateAfterTimeout: Option[HDuration],
  maximumRetries: Option[HInt],
  retryDelay: Option[HDuration],
  failureAndRerunMode: Option[FailureAndRerunMode],
  actionOnResourceFailure: Option[ActionOnResourceFailure],
  actionOnTaskFailure: Option[ActionOnTaskFailure]
) extends EmrActivity {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withSteps(step: SparkStep*) = this.copy(steps = steps ++ step)
  def withPreStepCommand(command: HString*) = this.copy(preStepCommands = preStepCommands ++ command)
  def withPostStepCommand(command: HString*) = this.copy(postStepCommands = postStepCommands ++ command)
  def withInput(input: S3DataNode*) = this.copy(inputs = inputs ++ input)
  def withOutput(output: S3DataNode*) = this.copy(outputs = outputs ++ output)

  private[hyperion] def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)
  def withAttemptTimeout(timeout: HDuration) = this.copy(attemptTimeout = Option(timeout))
  def withLateAfterTimeout(timeout: HDuration) = this.copy(lateAfterTimeout = Option(timeout))
  def withMaximumRetries(retries: HInt) = this.copy(maximumRetries = Option(retries))
  def withRetryDelay(delay: HDuration) = this.copy(retryDelay = Option(delay))
  def withFailureAndRerunMode(mode: FailureAndRerunMode) = this.copy(failureAndRerunMode = Option(mode))
  def withActionOnResourceFailure(action: ActionOnResourceFailure) = this.copy(actionOnResourceFailure = Option(action))
  def withActionOnTaskFailure(action: ActionOnTaskFailure) = this.copy(actionOnTaskFailure = Option(action))

  def objects: Iterable[PipelineObject] =
    runsOn.toSeq ++ inputs ++ outputs ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpEmrActivity(
    id = id,
    name = id.toOption,
    step = steps.map(_.toString),
    preStepCommand = seqToOption(preStepCommands)(_.toString),
    postStepCommand = seqToOption(postStepCommands)(_.toString),
    input = seqToOption(inputs)(_.ref),
    output = seqToOption(outputs)(_.ref),
    workerGroup = runsOn.asWorkerGroup.map(_.ref),
    runsOn = runsOn.asManagedResource.map(_.ref),
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref),
    attemptTimeout = attemptTimeout.map(_.toString),
    lateAfterTimeout = lateAfterTimeout.map(_.toString),
    maximumRetries = maximumRetries.map(_.toString),
    retryDelay = retryDelay.map(_.toString),
    failureAndRerunMode = failureAndRerunMode.map(_.toString),
    actionOnResourceFailure = actionOnResourceFailure.map(_.toString),
    actionOnTaskFailure = actionOnTaskFailure.map(_.toString)
  )
}

object SparkActivity extends RunnableObject {

  def apply(runsOn: Resource[SparkCluster]): SparkActivity =
    new SparkActivity(
      id = PipelineObjectId(SparkActivity.getClass),
      steps = Seq.empty,
      preStepCommands = Seq.empty,
      postStepCommands = Seq.empty,
      inputs = Seq.empty,
      outputs = Seq.empty,
      runsOn = runsOn,
      dependsOn = Seq.empty,
      preconditions = Seq.empty,
      onFailAlarms = Seq.empty,
      onSuccessAlarms = Seq.empty,
      onLateActionAlarms = Seq.empty,
      attemptTimeout = None,
      lateAfterTimeout = None,
      maximumRetries = None,
      retryDelay = None,
      failureAndRerunMode = None,
      actionOnResourceFailure = None,
      actionOnTaskFailure = None
    )
}
