package com.krux.hyperion.datanode

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.HString
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
  tableName: HString,
  createTableSql: Option[HString],
  schemaName: Option[HString],
  primaryKeys: Seq[HString],
  preconditions: Seq[Precondition],
  onSuccessAlarms: Seq[SnsAlarm],
  onFailAlarms: Seq[SnsAlarm]
) extends DataNode {

  def named(name: String): RedshiftDataNode = this.copy(id = id.named(name))
  def groupedBy(group: String): RedshiftDataNode = this.copy(id = id.groupedBy(group))

  def withCreateTableSql(createSql: HString): RedshiftDataNode = this.copy(createTableSql = Option(createSql))
  def withSchema(name: HString): RedshiftDataNode = this.copy(schemaName = Option(name))
  def withPrimaryKeys(pks: HString*): RedshiftDataNode = this.copy(primaryKeys = primaryKeys ++ pks)

  def whenMet(conditions: Precondition*): RedshiftDataNode = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*): RedshiftDataNode = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*): RedshiftDataNode = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)

  def objects: Iterable[PipelineObject] = Option(database) ++ preconditions ++ onSuccessAlarms ++ onFailAlarms

  lazy val serialize = AdpRedshiftDataNode(
    id = id,
    name = id.toOption,
    createTableSql = createTableSql.map(_.serialize),
    database = database.ref,
    schemaName = schemaName.map(_.serialize),
    tableName = tableName.serialize,
    primaryKeys = seqToOption(primaryKeys)(_.toString),
    precondition = seqToOption(preconditions)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref)
  )

}

object RedshiftDataNode {
  def apply(database: RedshiftDatabase, tableName: HString): RedshiftDataNode =
    new RedshiftDataNode(
      id = PipelineObjectId(RedshiftDataNode.getClass),
      database = database,
      tableName = tableName,
      createTableSql = None,
      schemaName = None,
      primaryKeys = Seq.empty,
      preconditions = Seq.empty,
      onSuccessAlarms = Seq.empty,
      onFailAlarms = Seq.empty
    )
}
