package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpJdbcDatabase

trait JdbcDatabase extends Database {

  def id: PipelineObjectId

  def username: String

  def `*password`: String

  def connectionString: String

  def jdbcDriverClass: String

  def databaseName: Option[String] = None

  def jdbcProperties: Seq[String] = Seq()

  def serialize = AdpJdbcDatabase(
      id = id,
      name = Some(id),
      connectionString = connectionString,
      jdbcDriverClass = jdbcDriverClass,
      databaseName = databaseName,
      jdbcProperties = jdbcProperties match {
        case Seq() => None
        case other => Some(other)
      },
      `*password` = `*password`,
      username = username
    )

}
