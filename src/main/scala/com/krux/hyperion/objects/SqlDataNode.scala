package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpSqlDataNode, AdpPrecondition, AdpSnsAlarm, AdpRef}
import com.krux.hyperion.objects.sql.{TableQuery, SelectTableQuery, InsertTableQuery}
import com.krux.hyperion.util.PipelineId

/**
 * @note that the AWS Datapipeline SqlDataNode does not require a JdbcDatabase parameter, but
 * requires specify the username, password, etc. within the object, we require a JdbcDatabase
 * object for consistency with other database data node objects.
 */
case class SqlDataNode (
  id: String,
  tableQuery: TableQuery,
  database: JdbcDatabase,
  preconditions: Seq[Precondition] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onFailAlarms: Seq[SnsAlarm] = Seq()
) extends Copyable {

  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)

  def serialize = AdpSqlDataNode(
    id = id,
    name = Some(id),
    table = tableQuery.table,
    username = database.username,
    `*password` = database.`*password`,
    connectionString = database.connectionString,
    selectQuery = tableQuery match {
      case q: SelectTableQuery => Some(q.sql)
      case _ => None
    },
    insertQuery = tableQuery match {
      case q: InsertTableQuery => Some(q.sql)
      case _ => None
    },
    precondition = preconditions match {
      case Seq() => None
      case conditions => Some(conditions.map(precondition => AdpRef[AdpPrecondition](precondition.id)))
    },
    onSuccess = onSuccessAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    },
    onFail = onFailAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    }
  )

}

object SqlDataNode {

  def apply(tableQuery: TableQuery, database: JdbcDatabase) =
    new SqlDataNode(
      id = PipelineId.generateNewId("SqlDataNode"),
      tableQuery = tableQuery,
      database = database,
      preconditions = Seq(),
      onSuccessAlarms = Seq(),
      onFailAlarms = Seq()
    )

}
