package com.krux.hyperion.datanode

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpSqlDataNode
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}
import com.krux.hyperion.database.Database
import com.krux.hyperion.precondition.Precondition

/**
 * @note that the AWS Datapipeline SqlDataNode does not require a JdbcDatabase parameter, but
 * requires specify the username, password, etc. within the object, we require a JdbcDatabase
 * object for consistency with other database data node objects.
 */
case class SqlDataNode (
  id: PipelineObjectId,
  tableQuery: TableQuery,
  database: Database,
  preconditions: Seq[Precondition],
  onSuccessAlarms: Seq[SnsAlarm],
  onFailAlarms: Seq[SnsAlarm]
) extends Copyable {

  def named(name: String): SqlDataNode = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String): SqlDataNode = this.copy(id = PipelineObjectId.withGroup(group, id))

  def whenMet(conditions: Precondition*): SqlDataNode = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*): SqlDataNode = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*): SqlDataNode = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)

  def objects: Iterable[PipelineObject] = Some(database) ++ preconditions ++ onSuccessAlarms ++ onFailAlarms

  lazy val serialize = AdpSqlDataNode(
    id = id,
    name = id.toOption,
    database = database.ref,
    table = tableQuery.table,
    selectQuery = tableQuery match {
      case q: SelectTableQuery => Option(q.sql)
      case _ => None
    },
    insertQuery = tableQuery match {
      case q: InsertTableQuery => Option(q.sql)
      case _ => None
    },
    precondition = seqToOption(preconditions)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref)
  )

}

object SqlDataNode {

  def apply(tableQuery: TableQuery, database: Database): SqlDataNode =
   new SqlDataNode(
      id = PipelineObjectId(SqlDataNode.getClass),
      tableQuery = tableQuery,
      database = database,
      preconditions = Seq(),
      onSuccessAlarms = Seq(),
      onFailAlarms = Seq()
    )

}
