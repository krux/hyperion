package com.krux.hyperion.h3.datanode

import com.krux.hyperion.adt.HString
import com.krux.hyperion.aws.AdpRedshiftDataNode
import com.krux.hyperion.h3.database.RedshiftDatabase
import com.krux.hyperion.h3.common.{ ObjectFields, PipelineObjectId }

/**
 * The abstracted RedshiftDataNode
 */
case class RedshiftDataNode private (
  baseFields: ObjectFields,
  dataNodeFields: DataNodeFields,
  database: RedshiftDatabase,
  tableName: HString,
  createTableSql: Option[HString],
  schemaName: Option[HString],
  primaryKeys: Seq[HString]
) extends DataNode {

  type Self = RedshiftDataNode

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateDataNodeFields(fields: DataNodeFields) = copy(dataNodeFields = fields)

  def withCreateTableSql(createSql: HString): RedshiftDataNode = this.copy(createTableSql = Option(createSql))
  def withSchema(name: HString): RedshiftDataNode = this.copy(schemaName = Option(name))
  def withPrimaryKeys(pks: HString*): RedshiftDataNode = this.copy(primaryKeys = primaryKeys ++ pks)

  // def objects: Iterable[PipelineObject] = Option(database) ++ preconditions ++ onSuccessAlarms ++ onFailAlarms
  def objects = None

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
      baseFields = ObjectFields(PipelineObjectId(RedshiftDataNode.getClass)),
      dataNodeFields = DataNodeFields(),
      database = database,
      tableName = tableName,
      createTableSql = None,
      schemaName = None,
      primaryKeys = Seq.empty
    )

}
