package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpRedshiftCopyActivity
import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.datanode.{S3DataNode, RedshiftDataNode}
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.Ec2Resource

/**
 * Copies data directly from DynamoDB or Amazon S3 to Amazon Redshift. You can load data into a new
 * table, or easily merge data into an existing table.
 */
case class RedshiftCopyActivity private (
  id: PipelineObjectId,
  input: S3DataNode,
  output: RedshiftDataNode,
  insertMode: RedshiftCopyActivity.InsertMode,
  runsOn: Ec2Resource,
  transformSql: Option[String] = None,
  commandOptions: Seq[RedshiftCopyOption] = Seq(),
  dependsOn: Seq[PipelineActivity] = Seq(),
  preconditions: Seq[Precondition] = Seq(),
  onFailAlarms: Seq[SnsAlarm] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onLateActionAlarms: Seq[SnsAlarm] = Seq()
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withCommandOptions(opts: RedshiftCopyOption*) = this.copy(commandOptions = commandOptions ++ opts)
  def withTransformSql(sql: String) = this.copy(transformSql = Option(sql))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)

  override def objects: Iterable[PipelineObject] = Seq(input, runsOn, output) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpRedshiftCopyActivity(
    id = id,
    name = id.toOption,
    input = input.ref,
    insertMode = insertMode.toString,
    output = output.ref,
    transformSql = transformSql,
    commandOptions = seqToOption(commandOptions)(_.repr).map(_.flatten),
    queue = None,
    runsOn = runsOn.ref,
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref)
  )

}

object RedshiftCopyActivity extends Enumeration with RunnableObject {

  type InsertMode = Value
  val KeepExisting = Value("KEEP_EXISTING")
  val OverwriteExisting = Value("OVERWRITE_EXISTING")
  val Truncate = Value("TRUNCATE")

  def apply(input: S3DataNode, output: RedshiftDataNode, insertMode: InsertMode, runsOn: Ec2Resource) =
    new RedshiftCopyActivity(
      id = PipelineObjectId("RedshiftCopyActivity"),
      input = input,
      output = output,
      insertMode = insertMode,
      runsOn = runsOn,
      transformSql = None,
      commandOptions = Seq(),
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )

}
