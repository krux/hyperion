package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpPigActivity
import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.datanode.DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.adt.{HInt, HDuration, HString, HS3Uri, HBoolean}
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, EmrCluster}

/**
 * PigActivity provides native support for Pig scripts in AWS Data Pipeline without the requirement
 * to use ShellCommandActivity or EmrActivity. In addition, PigActivity supports data staging. When
 * the stage field is set to true, AWS Data Pipeline stages the input data as a schema in Pig
 * without additional code from the user.
 */
case class PigActivity private (
  id: PipelineObjectId,
  script: Script,
  scriptVariables: Seq[HString],
  generatedScriptsPath: Option[HS3Uri],
  stage: Option[HBoolean],
  input: Option[DataNode],
  output: Option[DataNode],
  hadoopQueue: Option[HString],
  preActivityTaskConfig: Option[ShellScriptConfig],
  postActivityTaskConfig: Option[ShellScriptConfig],
  runsOn: Resource[EmrCluster],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm],
  attemptTimeout: Option[HDuration],
  lateAfterTimeout: Option[HDuration],
  maximumRetries: Option[HInt],
  retryDelay: Option[HDuration],
  failureAndRerunMode: Option[FailureAndRerunMode]
) extends PipelineActivity {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withScriptVariable(scriptVariable: HString*) = this.copy(scriptVariables = scriptVariables ++ scriptVariable)
  def withGeneratedScriptsPath(generatedScriptsPath: HS3Uri) = this.copy(generatedScriptsPath = Option(generatedScriptsPath))
  def withInput(in: DataNode) = this.copy(input = Option(in), stage = Option(HBoolean.True))
  def withOutput(out: DataNode) = this.copy(output = Option(out), stage = Option(HBoolean.True))
  def withHadoopQueue(queue: HString) = this.copy(hadoopQueue = Option(queue))
  def withPreActivityTaskConfig(script: ShellScriptConfig) = this.copy(preActivityTaskConfig = Option(script))
  def withPostActivityTaskConfig(script: ShellScriptConfig) = this.copy(postActivityTaskConfig = Option(script))

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

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = new AdpPigActivity(
    id = id,
    name = id.toOption,
    script = script.content.map(_.serialize),
    scriptUri = script.uri.map(_.serialize),
    scriptVariable = seqToOption(scriptVariables)(_.serialize),
    generatedScriptsPath = generatedScriptsPath.map(_.serialize),
    stage = stage.map(_.serialize),
    input = input.map(_.ref),
    output = output.map(_.ref),
    hadoopQueue = hadoopQueue.map(_.serialize),
    preActivityTaskConfig = preActivityTaskConfig.map(_.ref),
    postActivityTaskConfig = postActivityTaskConfig.map(_.ref),
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

object PigActivity extends RunnableObject {
  def apply(script: Script)(runsOn: Resource[EmrCluster]): PigActivity =
    new PigActivity(
      id = PipelineObjectId(PigActivity.getClass),
      script = script,
      scriptVariables = Seq.empty,
      generatedScriptsPath = None,
      stage = None,
      input = None,
      output = None,
      hadoopQueue = None,
      preActivityTaskConfig = None,
      postActivityTaskConfig = None,
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
      failureAndRerunMode = None
    )
}
