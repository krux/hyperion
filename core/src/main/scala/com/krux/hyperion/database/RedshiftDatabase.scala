package com.krux.hyperion.database

import com.krux.hyperion.aws.{AdpRedshiftDatabase, AdpRef}

/**
 * Redshift database trait, to use this please extend with an object.
 */
trait RedshiftDatabase extends Database {

  def clusterId: String

  def username: String

  def `*password`: String

  def databaseName: String

  lazy val serialize = AdpRedshiftDatabase(
    id = id,
    name = id.toOption,
    clusterId = clusterId,
    connectionString = None,
    databaseName = Option(databaseName),
    username = username,
    `*password` = `*password`,
    jdbcProperties = None
  )

  override def ref: AdpRef[AdpRedshiftDatabase] = AdpRef(serialize)

}
