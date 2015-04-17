package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpSqlDataNode, AdpPrecondition, AdpSnsAlarm, AdpRef}
import com.krux.hyperion.objects.sql.{TableQuery, SelectTableQuery, InsertTableQuery}

/**
 * @note that the AWS Datapipeline SqlDataNode does not require a JdbcDatabase parameter, but
 * requires specify the username, password, etc. within the object, we require a JdbcDatabase
 * object for consistency with other database data node objects.
 */
case class SqlDataNode (
  id: PipelineObjectId,
  tableQuery: TableQuery,
  database: JdbcDatabase,
  preconditions: Seq[Precondition],
  onSuccessAlarms: Seq[SnsAlarm],
  onFailAlarms: Seq[SnsAlarm]
) extends Copyable {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))

  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)

  lazy val serialize = AdpSqlDataNode(
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
    precondition = seqToOption(preconditions)(precondition => AdpRef(precondition.serialize)),
    onSuccess = seqToOption(onSuccessAlarms)(alarm => AdpRef(alarm.serialize)),
    onFail = seqToOption(onFailAlarms)(alarm => AdpRef(alarm.serialize))
  )

}

object SqlDataNode {

  def apply(tableQuery: TableQuery, database: JdbcDatabase) =
    new SqlDataNode(
      id = PipelineObjectId("SqlDataNode"),
      tableQuery = tableQuery,
      database = database,
      preconditions = Seq(),
      onSuccessAlarms = Seq(),
      onFailAlarms = Seq()
    )

}
