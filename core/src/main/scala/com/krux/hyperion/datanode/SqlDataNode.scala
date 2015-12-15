package com.krux.hyperion.datanode

import com.krux.hyperion.aws.AdpSqlDataNode
import com.krux.hyperion.database.Database
import com.krux.hyperion.common.{ BaseFields, PipelineObjectId }

/**
 * @note that the AWS Datapipeline SqlDataNode does not require a JdbcDatabase parameter, but
 * requires specify the username, password, etc. within the object, we require a JdbcDatabase
 * object for consistency with other database data node objects.
 */
case class SqlDataNode (
  baseFields: BaseFields,
  dataNodeFields: DataNodeFields,
  tableQuery: TableQuery,
  database: Database
) extends Copyable {

  type Self = SqlDataNode

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateDataNodeFields(fields: DataNodeFields) = copy(dataNodeFields = fields)

  override def objects = Seq(database) ++ super.objects

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
      baseFields = BaseFields(PipelineObjectId(SqlDataNode.getClass)),
      dataNodeFields = DataNodeFields(),
      tableQuery = tableQuery,
      database = database
    )

}
