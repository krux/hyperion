package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpSqlActivity
import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.database.RedshiftDatabase
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.Ec2Resource

/**
 * Runs an SQL query on a RedShift cluster. If the query writes out to a table that does not exist,
 * a new table with that name is created.
 */
case class SqlActivity private (
  id: PipelineObjectId,
  runsOn: Ec2Resource,
  database: RedshiftDatabase,
  script: String,
  scriptArgument: Seq[String],
  queue: Option[String],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm]
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withArguments(arg: String*) = this.copy(scriptArgument = scriptArgument ++ arg)
  def withQueue(queue: String) = this.copy(queue = Option(queue))

  private[hyperion] def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)

  override def objects: Iterable[PipelineObject] =
    Seq(runsOn, database) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpSqlActivity(
    id = id,
    name = id.toOption,
    database = database.ref,
    script = script,
    scriptArgument = scriptArgument,
    queue = queue,
    runsOn = runsOn.ref,
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref)
  )
}

object SqlActivity extends RunnableObject {
  def apply(runsOn: Ec2Resource, database: RedshiftDatabase, script: String) =
    new SqlActivity(
      id = PipelineObjectId("SqlActivity"),
      runsOn = runsOn,
      database = database,
      script = script,
      scriptArgument = Seq(),
      queue = None,
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )
}
