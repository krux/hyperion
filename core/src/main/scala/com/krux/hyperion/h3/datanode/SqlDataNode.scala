package com.krux.hyperion.h3.datanode

import shapeless._

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpSqlDataNode
import com.krux.hyperion.database.Database
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.h3.common.{ ObjectFields, PipelineObjectId }

/**
 * @note that the AWS Datapipeline SqlDataNode does not require a JdbcDatabase parameter, but
 * requires specify the username, password, etc. within the object, we require a JdbcDatabase
 * object for consistency with other database data node objects.
 */
case class SqlDataNode (
  baseFields: ObjectFields,
  dataNodeFields: DataNodeFields,
  tableQuery: TableQuery,
  database: Database
) extends Copyable {

  type Self = SqlDataNode

  def baseFieldsLens = lens[Self] >> 'baseFields
  def dataNodeFieldsLens = lens[Self] >> 'dataNodeFields

  def objects = None
  // def objects: Iterable[PipelineObject] = Some(database) ++ preconditions ++ onSuccessAlarms ++ onFailAlarms

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
      baseFields = ObjectFields(PipelineObjectId(SqlDataNode.getClass)),
      dataNodeFields = DataNodeFields(),
      tableQuery = tableQuery,
      database = database
    )

}
