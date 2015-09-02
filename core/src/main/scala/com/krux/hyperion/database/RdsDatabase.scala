package com.krux.hyperion.database

import com.krux.hyperion.aws.AdpRdsDatabase
import com.krux.hyperion.common.PipelineObjectId

trait RdsDatabase extends Database {

  def id: PipelineObjectId

  def databaseName: Option[String] = None

  def username: String

  def `*password`: String

  def jdbcDriverJarUri: Option[String] = None

  def jdbcProperties: Seq[String] = Seq.empty

  def rdsInstanceId: String

  def region: String

  lazy val serialize = AdpRdsDatabase(
    id = id,
    name = id.toOption,
    databaseName = databaseName,
    jdbcProperties = Option(jdbcProperties),
    username = username,
    `*password` = `*password`,
    rdsInstanceId = rdsInstanceId,
    region = region,
    jdbcDriverJarUri = jdbcDriverJarUri
  )
}
