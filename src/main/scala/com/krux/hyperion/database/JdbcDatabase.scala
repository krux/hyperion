package com.krux.hyperion.database

import com.krux.hyperion.aws.AdpJdbcDatabase
import com.krux.hyperion.common.PipelineObjectId

/**
 * Defines a JDBC database
 */
trait JdbcDatabase extends Database {

  def id: PipelineObjectId

  def username: String

  def `*password`: String

  def connectionString: String

  def jdbcDriverClass: String

  def databaseName: Option[String] = None

  def jdbcProperties: Seq[String] = Seq()

  lazy val serialize = AdpJdbcDatabase(
    id = id,
    name = id.toOption,
    connectionString = connectionString,
    jdbcDriverClass = jdbcDriverClass,
    databaseName = databaseName,
    jdbcProperties = jdbcProperties,
    `*password` = `*password`,
    username = username
  )

}
