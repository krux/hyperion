package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpRedshiftDataNode, AdpJsonSerializer, AdpRef, AdpPrecondition, AdpSnsAlarm}

/**
 * The abstracted RedshiftDataNode
 */
case class RedshiftDataNode private (
  id: PipelineObjectId,
  database: RedshiftDatabase,
  tableName: String,
  createTableSql: Option[String],
  schemaName: Option[String],
  primaryKeys: Option[Seq[String]],
  preconditions: Seq[Precondition],
  onSuccessAlarms: Seq[SnsAlarm],
  onFailAlarms: Seq[SnsAlarm]
) extends DataNode {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withCreateTableSql(createSql: String) = this.copy(createTableSql = Some(createSql))
  def withSchema(theSchemaName: String) = this.copy(schemaName = Some(theSchemaName))
  def withPrimaryKeys(pks: String*) = this.copy(primaryKeys = Some(pks))
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Some(database)

  lazy val serialize = AdpRedshiftDataNode(
    id = id,
    name = Some(id),
    createTableSql = createTableSql,
    database = AdpRef(database.serialize),
    schemaName = schemaName,
    tableName = tableName,
    primaryKeys = primaryKeys,
    precondition = seqToOption(preconditions)(precondition => AdpRef(precondition.serialize)),
    onSuccess = seqToOption(onSuccessAlarms)(alarm => AdpRef(alarm.serialize)),
    onFail = seqToOption(onFailAlarms)(alarm => AdpRef(alarm.serialize))
  )

}

object RedshiftDataNode {
  def apply(database: RedshiftDatabase, tableName: String) =
    new RedshiftDataNode(
      id = PipelineObjectId("RedshiftDataNode"),
      database = database,
      tableName = tableName,
      createTableSql = None,
      schemaName = None,
      primaryKeys = None,
      preconditions = Seq(),
      onSuccessAlarms = Seq(),
      onFailAlarms = Seq()
    )
}
