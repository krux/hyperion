package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpJdbcDatabase

trait JdbcDatabase extends Database {

  def id: String

  def username: String

  def `*password`: String

  def connectionString: String

  def jdbcDriverClass: String

  def serialize = AdpJdbcDatabase(
      id = id,
      name = Some(id),
      connectionString = connectionString,
      jdbcDriverClass = jdbcDriverClass,
      databaseName = None,
      jdbcProperties = None,
      `*password` = `*password`,
      username = username
    )

}
