package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.{AdpPigActivity, AdpDataNode, AdpRef, AdpEmrCluster, AdpActivity}

case class PigActivity(
  id: String,
  runsOn: EmrCluster,
  generatedScriptsPath: Option[String] = None,
  script: Option[String] = None,
  scriptUri: Option[String] = None,
  scriptVariable: Option[String] = None,
  input: Option[DataNode] = None,
  output: Option[DataNode] = None,
  stage: Option[Boolean] = None,
  dependsOn: Seq[PipelineActivity] = Seq()
)(
    implicit val hc: HyperionContext
  ) extends PipelineActivity {

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def withGeneratedScriptsPath(generatedScriptsPath: String) = this.copy(generatedScriptsPath = Some(generatedScriptsPath))
  def withScript(script: String) = this.copy(script = Some(script))
  def withScriptUri(scriptUri: String) = this.copy(scriptUri = Some(scriptUri))
  def withScriptVariable(scriptVariable: String) = this.copy(scriptVariable = Some(scriptVariable))

  def withInput(in: DataNode) = this.copy(input = Some(in))
  def withOutput(out: DataNode) = this.copy(output = Some(out))

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ output ++ dependsOn

  def serialize = AdpPigActivity(
    id,
    Some(id),
    generatedScriptsPath,
    script,
    scriptUri,
    scriptVariable,
    input.map(in => AdpRef[AdpDataNode](in.id)).get,
    output.map(out => AdpRef[AdpDataNode](out.id)).get,
    stage.toString,
    AdpRef[AdpEmrCluster](runsOn.id),
    dependsOn match {
      case Seq() => None
      case deps => Some(deps.map(act => AdpRef[AdpActivity](act.id)))
    }
  )
}