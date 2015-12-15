package com.krux.hyperion.database

import com.krux.hyperion.adt.HString
import com.krux.hyperion.aws.AdpRdsDatabase
import com.krux.hyperion.common.{ PipelineObjectId, BaseFields }

case class RdsDatabase private (
  baseFields: BaseFields,
  databaseFields: DatabaseFields,
  rdsInstanceId: HString,
  jdbcDriverJarUri: Option[HString],
  jdbcProperties: Seq[HString],
  region: Option[HString]
) extends Database {

  type Self = RdsDatabase

  def updateBaseFields(fields: BaseFields): Self = copy(baseFields = fields)
  def updateDatabaseFields(fields: DatabaseFields): Self = copy(databaseFields = fields)

  def withJdbcDriverJarUri(uri: HString): Self = copy(jdbcDriverJarUri = Option(uri))
  def withJdbcProperties(props: HString*): Self = copy(jdbcProperties = props)
  def withRegion(r: HString): Self = copy(region = Option(r))

  lazy val serialize = AdpRdsDatabase(
    id = id,
    name = id.toOption,
    databaseName = databaseName.map(_.serialize),
    jdbcProperties = jdbcProperties.map(_.serialize),
    username = username.serialize,
    `*password` = `*password`.serialize,
    rdsInstanceId = rdsInstanceId.serialize,
    region = region.map(_.serialize),
    jdbcDriverJarUri = jdbcDriverJarUri.map(_.serialize)
  )

}

object RdsDatabase {

  def apply(
    username: HString,
    password: HString,
    rdsInstanceId: HString
  ) = new RdsDatabase(
    baseFields = BaseFields(PipelineObjectId(RdsDatabase.getClass)),
    databaseFields = DatabaseFields(username = username, `*password` = password),
    rdsInstanceId = rdsInstanceId,
    jdbcDriverJarUri = None,
    jdbcProperties = Seq.empty,
    region = None
  )

}
