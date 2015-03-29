package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.{AdpHiveCopyActivity, AdpDataNode, AdpRef, AdpEmrCluster, AdpActivity}
import com.krux.hyperion.objects.aws.AdpSnsAlarm

case class HiveCopyActivity(
  id: String,
  runsOn: EmrCluster,
  filterSql: Option[String] = None,
  generatedScriptsPath: Option[String] = None,
  input: Option[DataNode] = None,
  output: Option[DataNode] = None,
  dependsOn: Seq[PipelineActivity] = Seq(),
  onFailAlarms: Seq[SnsAlarm] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onLateActionAlarms: Seq[SnsAlarm] = Seq()
)(
  implicit val hc: HyperionContext
) extends PipelineActivity {

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def withFilterSql(filterSql: String) = this.copy(filterSql = Some(filterSql))
  def withGeneratedScriptsPath(generatedScriptsPath: String) = this.copy(generatedScriptsPath = Some(generatedScriptsPath))

  def withInput(in: DataNode) = this.copy(input = Some(in))
  def withOutput(out: DataNode) = this.copy(output = Some(out))

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ output ++ dependsOn ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpHiveCopyActivity(
    id,
    Some(id),
    filterSql,
    generatedScriptsPath,
    input.map(in => AdpRef[AdpDataNode](in.id)).get,
    output.map(out => AdpRef[AdpDataNode](out.id)).get,
    AdpRef[AdpEmrCluster](runsOn.id),
    dependsOn match {
      case Seq() => None
      case deps => Some(deps.map(act => AdpRef[AdpActivity](act.id)))
    },
    onFailAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    },
    onSuccessAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    },
    onLateActionAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    }
  )
}