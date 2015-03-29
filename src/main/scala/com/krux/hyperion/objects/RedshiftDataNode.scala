package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpRedshiftDataNode, AdpJsonSerializer, AdpRef, AdpPrecondition, AdpSnsAlarm}

/**
 * The abstracted RedshiftDataNode
 */
case class RedshiftDataNode (
  id: String,
  database: RedshiftDatabase,
  tableName: String,
  createTableSql: Option[String] = None,
  schemaName: Option[String] = None,
  primaryKeys: Option[Seq[String]] = None,
  preconditions: Seq[Precondition] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onFailAlarms: Seq[SnsAlarm] = Seq()
) extends DataNode {

  def withCreateTableSql(createSql: String) = this.copy(createTableSql = Some(createSql))
  def withSchema(theSchemaName: String) = this.copy(schemaName = Some(theSchemaName))
  def withPrimaryKeys(pks: String*) = this.copy(primaryKeys = Some(pks))
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Some(database)

  def serialize = AdpRedshiftDataNode(
    id = id,
    name = Some(id),
    createTableSql = createTableSql,
    database = AdpRef(database.id),
    schemaName = schemaName,
    tableName = tableName,
    primaryKeys = primaryKeys,
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
