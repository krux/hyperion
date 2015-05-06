package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpHiveCopyActivity

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

  def withFilterSql(filterSql: String) = this.copy(filterSql = Option(filterSql))
  def withGeneratedScriptsPath(generatedScriptsPath: String) = this.copy(generatedScriptsPath = Option(generatedScriptsPath))

  def withInput(in: DataNode) = this.copy(input = Option(in))
  def withOutput(out: DataNode) = this.copy(output = Option(out))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpHiveCopyActivity(
    id = id,
    name = id.toOption,
    filterSql = filterSql,
    generatedScriptsPath = generatedScriptsPath,
    input = input.map(_.ref).get,
    output = output.map(_.ref).get,
    runsOn = runsOn.ref,
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref)
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
