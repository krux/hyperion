package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.{AdpHiveActivity, AdpDataNode, AdpRef, AdpEmrCluster, AdpActivity, AdpPrecondition}
import com.krux.hyperion.objects.aws.AdpSnsAlarm

case class HiveActivity(
  id: String,
  runsOn: EmrCluster,
  hiveScript: Option[String] = None,
  scriptUri: Option[String] = None,
  scriptVariable: Option[String] = None,
  input: Option[DataNode] = None,
  output: Option[DataNode] = None,
  stage: Option[Boolean] = None,
  dependsOn: Seq[PipelineActivity] = Seq(),
  preconditions: Seq[Precondition] = Seq(),
  onFailAlarms: Seq[SnsAlarm] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onLateActionAlarms: Seq[SnsAlarm] = Seq()
)(
  implicit val hc: HyperionContext
) extends PipelineActivity {

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def withHiveScript(hiveScript: String) = this.copy(hiveScript = Some(hiveScript))
  def withScriptUri(scriptUri: String) = this.copy(scriptUri = Some(scriptUri))
  def withScriptVariable(scriptVariable: String) = this.copy(scriptVariable = Some(scriptVariable))

  def withInput(in: DataNode) = this.copy(input = Some(in))
  def withOutput(out: DataNode) = this.copy(output = Some(out))

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpHiveActivity(
    id = id,
    name = Some(id),
    hiveScript = hiveScript,
    scriptUri = scriptUri,
    scriptVariable = scriptVariable,
    input = input.map(in => AdpRef[AdpDataNode](in.id)).get,
    output = output.map(out => AdpRef[AdpDataNode](out.id)).get,
    stage = stage.toString,
    runsOn = AdpRef[AdpEmrCluster](runsOn.id),
    dependsOn match {
      case Seq() => None
      case deps => Some(deps.map(act => AdpRef[AdpActivity](act.id)))
    },
    precondition = preconditions match {
      case Seq() => None
      case preconditions => Some(preconditions.map(precondition => AdpRef[AdpPrecondition](precondition.id)))
    },
    onFail = onFailAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    },
    onSuccess = onSuccessAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    },
    onLateAction = onLateActionAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    }
  )
}