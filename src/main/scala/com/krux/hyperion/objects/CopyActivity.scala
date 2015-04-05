package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.{AdpCopyActivity, AdpDataNode, AdpRef, AdpEc2Resource,
  AdpActivity, AdpS3DataNode, AdpSqlDataNode, AdpSnsAlarm, AdpPrecondition}


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

  def serialize = AdpCopyActivity(
    id = id,
    name = Some(id),
    input = input match {
      case n: S3DataNode => AdpRef[AdpS3DataNode](n.id)
      case n: SqlDataNode => AdpRef[AdpSqlDataNode](n.id)
    },
    output = output match {
      case n: S3DataNode => AdpRef[AdpS3DataNode](n.id)
      case n: SqlDataNode => AdpRef[AdpSqlDataNode](n.id)
    },
    runsOn = AdpRef[AdpEc2Resource](runsOn.id),
    dependsOn = dependsOn match {
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
