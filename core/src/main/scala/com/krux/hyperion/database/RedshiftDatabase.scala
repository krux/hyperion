package com.krux.hyperion.database

import com.krux.hyperion.adt.HString
import com.krux.hyperion.aws.{ AdpRedshiftDatabase, AdpRef }
import com.krux.hyperion.common.{ PipelineObjectId, ObjectFields }

/**
 * Redshift database trait, to use this please extend with an object.
 */
case class RedshiftDatabase private (
  baseFields: ObjectFields,
  databaseFields: DatabaseFields,
  clusterId: HString
) extends Database {

  type Self = RedshiftDatabase

  def updateBaseFields(fields: ObjectFields): Self = copy(baseFields = fields)
  def updateDatabaseFields(fields: DatabaseFields): Self = copy(databaseFields = fields)

  lazy val serialize = AdpRedshiftDatabase(
    id = id,
    name = id.toOption,
    clusterId = clusterId.serialize,
    connectionString = None,
    databaseName = databaseName.map(_.serialize),
    username = username.serialize,
    `*password` = `*password`.serialize,
    jdbcProperties = None
  )

  override def ref: AdpRef[AdpRedshiftDatabase] = AdpRef(serialize)

}

object RedshiftDatabase {

  def apply(
    username: HString,
    password: HString,
    clusterId: HString
  ) = new RedshiftDatabase(
    baseFields = ObjectFields(PipelineObjectId(RdsDatabase.getClass)),
    databaseFields = DatabaseFields(username = username, `*password` = password),
    clusterId = clusterId
  )

}
