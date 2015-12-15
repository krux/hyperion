package com.krux.hyperion.database

import com.krux.hyperion.adt.HString
import com.krux.hyperion.aws.AdpJdbcDatabase
import com.krux.hyperion.common.{ PipelineObjectId, BaseFields }

/**
 * Defines a JDBC database
 */
case class JdbcDatabase private (
  baseFields: BaseFields,
  databaseFields: DatabaseFields,
  connectionString: HString,
  jdbcDriverClass: HString,
  jdbcDriverJarUri: Option[HString],
  jdbcProperties: Seq[HString]
) extends Database {

  type Self = JdbcDatabase

  def updateBaseFields(fields: BaseFields): Self = copy(baseFields = fields)
  def updateDatabaseFields(fields: DatabaseFields): Self = copy(databaseFields = fields)

  def withJdbcDriverJarUri(uri: HString): Self = copy(jdbcDriverJarUri = Option(uri))
  def withJdbcProperties(props: HString*): Self = copy(jdbcProperties = jdbcProperties ++ props)

  lazy val serialize = AdpJdbcDatabase(
    id = id,
    name = id.toOption,
    connectionString = connectionString.serialize,
    databaseName = databaseName.map(_.serialize),
    username = username.serialize,
    `*password` = `*password`.serialize,
    jdbcDriverJarUri = jdbcDriverJarUri.map(_.serialize),
    jdbcDriverClass = jdbcDriverClass.serialize,
    jdbcProperties = jdbcProperties.map(_.serialize)
  )

}

object JdbcDatabase {

  def apply(
    username: HString,
    password: HString,
    connectionString: HString,
    jdbcDriverClass: HString
  ) = new JdbcDatabase(
    baseFields = BaseFields(PipelineObjectId(JdbcDatabase.getClass)),
    databaseFields = DatabaseFields(username = username, `*password` = password),
    connectionString = connectionString,
    jdbcDriverClass = jdbcDriverClass,
    jdbcDriverJarUri = None,
    jdbcProperties = Seq.empty
  )

}
