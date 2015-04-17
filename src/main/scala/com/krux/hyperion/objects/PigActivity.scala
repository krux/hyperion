package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpPigActivity

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
)(
  implicit val hc: HyperionContext
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))

  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withGeneratedScriptsPath(generatedScriptsPath: String) = this.copy(generatedScriptsPath = Some(generatedScriptsPath))
  def withScript(script: String) = this.copy(script = Some(script))
  def withScriptUri(scriptUri: String) = this.copy(scriptUri = Some(scriptUri))
  def withScriptVariable(scriptVariable: String) = this.copy(scriptVariable = Some(scriptVariable))

  def withInput(in: DataNode) = this.copy(input = Some(in))
  def withOutput(out: DataNode) = this.copy(output = Some(out))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpPigActivity(
    id = id,
    name = Some(id),
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

object PigActivity {
  def apply(runsOn: EmrCluster)(implicit hc: HyperionContext) =
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
