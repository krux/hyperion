package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpSqlDataNode
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
    database: JdbcDatabase
  ) extends Copyable {

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
      }
    )
}

object SqlDataNode {
  def apply(tableQuery: TableQuery, database: JdbcDatabase) =
    new SqlDataNode(
      id = PipelineId.generateNewId("SqlDataNode"),
      tableQuery = tableQuery,
      database = database
    )
}
