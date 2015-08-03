package com.krux.hyperion.database

import com.krux.hyperion.aws.AdpJdbcDatabase
import com.krux.hyperion.common.PipelineObjectId

/**
 * Defines a JDBC database
 */
trait JdbcDatabase extends Database {

  def id: PipelineObjectId

  def connectionString: String

  def databaseName: Option[String] = None

  def username: String

  def `*password`: String

  def jdbcDriverJarUri: Option[String] = None

  def jdbcDriverClass: String

  def jdbcProperties: Seq[String] = Seq()

  lazy val serialize = AdpJdbcDatabase(
    id = id,
    name = id.toOption,
    connectionString = connectionString,
    databaseName = databaseName,
    username = username,
    `*password` = `*password`,
    jdbcDriverJarUri = jdbcDriverJarUri,
    jdbcDriverClass = jdbcDriverClass,
    jdbcProperties = jdbcProperties
  )

}
