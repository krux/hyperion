package com.krux.hyperion.h3.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.{ HString, HS3Uri, HBoolean }
import com.krux.hyperion.aws.AdpPigActivity
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.h3.common.{ PipelineObjectId, ObjectFields }
import com.krux.hyperion.h3.datanode.DataNode
import com.krux.hyperion.h3.resource.{ Resource, EmrCluster }

/**
 * PigActivity provides native support for Pig scripts in AWS Data Pipeline without the requirement
 * to use ShellCommandActivity or EmrActivity. In addition, PigActivity supports data staging. When
 * the stage field is set to true, AWS Data Pipeline stages the input data as a schema in Pig
 * without additional code from the user.
 */
case class PigActivity[A <: EmrCluster] private (
  baseFields: ObjectFields,
  activityFields: ActivityFields[A],
  script: Script,
  scriptVariables: Seq[HString],
  generatedScriptsPath: Option[HS3Uri],
  stage: Option[HBoolean],
  input: Option[DataNode],
  output: Option[DataNode],
  hadoopQueue: Option[HString],
  preActivityTaskConfig: Option[ShellScriptConfig],
  postActivityTaskConfig: Option[ShellScriptConfig]
) extends EmrActivity[A] {

  type Self = PigActivity[A]

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[A]) = copy(activityFields = fields)

  def withScriptVariable(scriptVariable: HString*) = this.copy(scriptVariables = scriptVariables ++ scriptVariable)
  def withGeneratedScriptsPath(generatedScriptsPath: HS3Uri) = this.copy(generatedScriptsPath = Option(generatedScriptsPath))
  def withInput(in: DataNode) = this.copy(input = Option(in), stage = Option(HBoolean.True))
  def withOutput(out: DataNode) = this.copy(output = Option(out), stage = Option(HBoolean.True))
  def withHadoopQueue(queue: HString) = this.copy(hadoopQueue = Option(queue))
  def withPreActivityTaskConfig(script: ShellScriptConfig) = this.copy(preActivityTaskConfig = Option(script))
  def withPostActivityTaskConfig(script: ShellScriptConfig) = this.copy(postActivityTaskConfig = Option(script))

  // def objects: Iterable[PipelineObject] = runsOn.toSeq ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

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

  def apply[A <: EmrCluster](script: Script)(runsOn: Resource[A]): PigActivity[A] =
    new PigActivity(
      baseFields = ObjectFields(PipelineObjectId(PigActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      script = script,
      scriptVariables = Seq.empty,
      generatedScriptsPath = None,
      stage = None,
      input = None,
      output = None,
      hadoopQueue = None,
      preActivityTaskConfig = None,
      postActivityTaskConfig = None
    )
}
