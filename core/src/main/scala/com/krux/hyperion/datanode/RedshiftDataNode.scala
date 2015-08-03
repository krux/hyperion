package com.krux.hyperion.datanode

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpRedshiftDataNode
import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.database.RedshiftDatabase
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.WorkerGroup

/**
 * The abstracted RedshiftDataNode
 */
case class RedshiftDataNode private (
  id: PipelineObjectId,
  database: RedshiftDatabase,
  tableName: String,
  createTableSql: Option[String],
  schemaName: Option[String],
  primaryKeys: Seq[String],
  preconditions: Seq[Precondition],
  onSuccessAlarms: Seq[SnsAlarm],
  onFailAlarms: Seq[SnsAlarm]
) extends DataNode {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withCreateTableSql(createSql: String) = this.copy(createTableSql = Option(createSql))
  def withSchema(name: String) = this.copy(schemaName = Option(name))
  def withPrimaryKeys(pks: String*) = this.copy(primaryKeys = primaryKeys ++ pks)

  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)

  def objects: Iterable[PipelineObject] = Option(database)

  lazy val serialize = AdpRedshiftDataNode(
    id = id,
    name = id.toOption,
    createTableSql = createTableSql,
    database = database.ref,
    schemaName = schemaName,
    tableName = tableName,
    primaryKeys = seqToOption(primaryKeys)(_.toString),
    precondition = seqToOption(preconditions)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref)
  )

}

object RedshiftDataNode {
  def apply(database: RedshiftDatabase, tableName: String): RedshiftDataNode =
    new RedshiftDataNode(
      id = PipelineObjectId(RedshiftDataNode.getClass),
      database = database,
      tableName = tableName,
      createTableSql = None,
      schemaName = None,
      primaryKeys = Seq(),
      preconditions = Seq(),
      onSuccessAlarms = Seq(),
      onFailAlarms = Seq()
    )
}
