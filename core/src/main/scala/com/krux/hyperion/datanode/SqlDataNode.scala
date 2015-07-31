package com.krux.hyperion.datanode

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpSqlDataNode
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.database.JdbcDatabase
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.WorkerGroup

/**
 * @note that the AWS Datapipeline SqlDataNode does not require a JdbcDatabase parameter, but
 * requires specify the username, password, etc. within the object, we require a JdbcDatabase
 * object for consistency with other database data node objects.
 */
case class SqlDataNode (
  id: PipelineObjectId,
  tableQuery: TableQuery,
  database: JdbcDatabase,
  workerGroup: Option[WorkerGroup],
  preconditions: Seq[Precondition],
  onSuccessAlarms: Seq[SnsAlarm],
  onFailAlarms: Seq[SnsAlarm]
) extends Copyable {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)

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
    workerGroup = workerGroup.map(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref)
  )

}

object SqlDataNode {

  def apply(tableQuery: TableQuery, database: JdbcDatabase): SqlDataNode = apply(tableQuery, database, None)

  def apply(tableQuery: TableQuery, database: JdbcDatabase, workerGroup: WorkerGroup): SqlDataNode =
    apply(tableQuery, database, Option(workerGroup))

  private def apply(tableQuery: TableQuery, database: JdbcDatabase, runsOn: Option[WorkerGroup]): SqlDataNode =
    new SqlDataNode(
      id = PipelineObjectId(SqlDataNode.getClass),
      tableQuery = tableQuery,
      database = database,
      workerGroup = runsOn,
      preconditions = Seq(),
      onSuccessAlarms = Seq(),
      onFailAlarms = Seq()
    )

}
