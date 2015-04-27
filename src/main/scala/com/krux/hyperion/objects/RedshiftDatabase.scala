package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpRedshiftDatabase, AdpRef}

/**
 * Redshift Database Trait, to use this please extend with an object.
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
    jdbcProperties = None,
    `*password` = `*password`,
    username = username
  )

  override def ref: AdpRef[AdpRedshiftDatabase] = AdpRef(serialize)

}
