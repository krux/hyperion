package com.krux.hyperion.activity

import com.krux.hyperion.adt.{HString, HS3Uri, HBoolean}
import com.krux.hyperion.aws.AdpPigActivity
import com.krux.hyperion.common.{PipelineObjectId, BaseFields}
import com.krux.hyperion.datanode.DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.resource.{Resource, BaseEmrCluster}

/**
 * PigActivity provides native support for Pig scripts in AWS Data Pipeline without the requirement
 * to use ShellCommandActivity or EmrActivity. In addition, PigActivity supports data staging. When
 * the stage field is set to true, AWS Data Pipeline stages the input data as a schema in Pig
 * without additional code from the user.
 */
case class PigActivity[A <: BaseEmrCluster] private (
  baseFields: BaseFields,
  activityFields: ActivityFields[A],
  emrTaskActivityFields: EmrTaskActivityFields,
  script: Script,
  scriptVariables: Seq[HString],
  generatedScriptsPath: Option[HS3Uri],
  stage: Option[HBoolean],
  input: Option[DataNode],
  output: Option[DataNode]
) extends EmrTaskActivity[A] {

  type Self = PigActivity[A]

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[A]) = copy(activityFields = fields)
  def updateEmrTaskActivityFields(fields: EmrTaskActivityFields) = copy(emrTaskActivityFields = fields)

  def withScriptVariable(scriptVariable: HString*) = copy(scriptVariables = scriptVariables ++ scriptVariable)
  def withGeneratedScriptsPath(generatedScriptsPath: HS3Uri) = copy(generatedScriptsPath = Option(generatedScriptsPath))
  def withInput(in: DataNode) = copy(input = Option(in), stage = Option(HBoolean.True))
  def withOutput(out: DataNode) = copy(output = Option(out), stage = Option(HBoolean.True))

  override def objects = input ++ output ++ super.objects

  lazy val serialize = new AdpPigActivity(
    id = id,
    name = name,
    script = script.content.map(_.serialize),
    scriptUri = script.uri.map(_.serialize),
    scriptVariable = seqToOption(scriptVariables)(_.serialize),
    generatedScriptsPath = generatedScriptsPath.map(_.serialize),
    stage = stage.map(_.serialize),
    input = input.map(_.ref),
    output = output.map(_.ref),
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
    failureAndRerunMode = failureAndRerunMode.map(_.serialize),
    maxActiveInstances = maxActiveInstances.map(_.serialize)
  )
}

object PigActivity extends RunnableObject {

  def apply[A <: BaseEmrCluster](script: Script)(runsOn: Resource[A]): PigActivity[A] =
    new PigActivity(
      baseFields = BaseFields(PipelineObjectId(PigActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      emrTaskActivityFields = EmrTaskActivityFields(),
      script = script,
      scriptVariables = Seq.empty,
      generatedScriptsPath = None,
      stage = None,
      input = None,
      output = None
    )
}
