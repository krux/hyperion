package com.krux.hyperion.activity

import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpPigActivity
import com.krux.hyperion.datanode.DataNode
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.EmrCluster

/**
 * PigActivity provides native support for Pig scripts in AWS Data Pipeline without the requirement
 * to use ShellCommandActivity or EmrActivity. In addition, PigActivity supports data staging. When
 * the stage field is set to true, AWS Data Pipeline stages the input data as a schema in Pig
 * without additional code from the user.
 */
case class PigActivity private (
  id: PipelineObjectId,
  runsOn: EmrCluster,
  generatedScriptsPath: Option[String],
  script: Option[String],
  scriptUri: Option[String],
  scriptVariable: Option[String],
  input: Option[DataNode],
  output: Option[DataNode],
  stage: Option[Boolean],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm]
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withGeneratedScriptsPath(generatedScriptsPath: String) = this.copy(generatedScriptsPath = Option(generatedScriptsPath))
  def withScript(script: String) = this.copy(script = Option(script))
  def withScriptUri(scriptUri: String) = this.copy(scriptUri = Option(scriptUri))
  def withScriptVariable(scriptVariable: String) = this.copy(scriptVariable = Option(scriptVariable))

  def withInput(in: DataNode) = this.copy(input = Option(in))
  def withOutput(out: DataNode) = this.copy(output = Option(out))

  private[hyperion] def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpPigActivity(
    id = id,
    name = id.toOption,
    generatedScriptsPath = generatedScriptsPath,
    script = script,
    scriptUri = scriptUri,
    scriptVariable = scriptVariable,
    input = input.map(_.ref).get,
    output = output.map(_.ref).get,
    stage = stage.toString,
    runsOn = runsOn.ref,
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref)
  )
}

object PigActivity extends RunnableObject {
  def apply(runsOn: EmrCluster) =
    new PigActivity(
      id = PipelineObjectId("PigActivity"),
      runsOn = runsOn,
      generatedScriptsPath = None,
      script = None,
      scriptUri = None,
      scriptVariable = None,
      input = None,
      output = None,
      stage = None,
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )
}
