package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpCopyActivity


/**
 * The activity that copys data from one data node to the other.
 *
 * @note it seems that both input and output format needs to be in CsvDataFormat for this copy to
 * work properly and it needs to be a specific variance of the CSV, for more information check the
 * web page:
 *
 * http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-copyactivity.html
 *
 * From our experience it's really hard to export using TsvDataFormat, in both import and export
 * especially for tasks involving RedshiftCopyActivity. A general rule of thumb is always use
 * default CsvDataFormat for tasks involving both exporting to S3 and copy to redshift.
 */
case class CopyActivity private (
  id: PipelineObjectId,
  input: Copyable,
  output: Copyable,
  runsOn: Ec2Resource,
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

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn, input, output) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpCopyActivity(
    id = id,
    name = Some(id),
    input = input match {
      case n: S3DataNode => n.ref
      case n: SqlDataNode => n.ref
    },
    output = output match {
      case n: S3DataNode => n.ref
      case n: SqlDataNode => n.ref
    },
    runsOn = runsOn.ref,
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref)
  )
}

object CopyActivity {

  def apply(input: Copyable, output: Copyable, runsOn: Ec2Resource)(implicit hc: HyperionContext) =
    new CopyActivity(
      id = PipelineObjectId("CopyActivity"),
      input = input,
      output = output,
      runsOn = runsOn,
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )

}
