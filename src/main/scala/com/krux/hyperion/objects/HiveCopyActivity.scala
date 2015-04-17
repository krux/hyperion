package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.{AdpHiveCopyActivity, AdpDataNode, AdpRef, AdpEmrCluster, AdpActivity, AdpPrecondition}
import com.krux.hyperion.objects.aws.AdpSnsAlarm

case class HiveCopyActivity private (
  id: PipelineObjectId,
  runsOn: EmrCluster,
  filterSql: Option[String],
  generatedScriptsPath: Option[String],
  input: Option[DataNode],
  output: Option[DataNode],
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

  def withFilterSql(filterSql: String) = this.copy(filterSql = Some(filterSql))
  def withGeneratedScriptsPath(generatedScriptsPath: String) = this.copy(generatedScriptsPath = Some(generatedScriptsPath))

  def withInput(in: DataNode) = this.copy(input = Some(in))
  def withOutput(out: DataNode) = this.copy(output = Some(out))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpHiveCopyActivity(
    id = id,
    name = Some(id),
    filterSql = filterSql,
    generatedScriptsPath = generatedScriptsPath,
    input = input.map(in => AdpRef(in.serialize)).get,
    output = output.map(out => AdpRef(out.serialize)).get,
    runsOn = AdpRef(runsOn.serialize),
    dependsOn = seqToOption(dependsOn)(act => AdpRef(act.serialize)),
    precondition = seqToOption(preconditions)(precondition => AdpRef(precondition.serialize)),
    onFail = seqToOption(onFailAlarms)(alarm => AdpRef(alarm.serialize)),
    onSuccess = seqToOption(onSuccessAlarms)(alarm => AdpRef(alarm.serialize)),
    onLateAction = seqToOption(onLateActionAlarms)(alarm => AdpRef(alarm.serialize))
  )
}

object HiveCopyActivity {
  def apply(runsOn: EmrCluster)(implicit hc: HyperionContext) =
    new HiveCopyActivity(
      id = PipelineObjectId("HiveCopyActivity"),
      runsOn = runsOn,
      filterSql = None,
      generatedScriptsPath = None,
      input = None,
      output = None,
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )
}
