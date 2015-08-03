package com.krux.hyperion.dataformat

import com.krux.hyperion.aws.AdpDynamoDBDataFormat
import com.krux.hyperion.common.PipelineObjectId

/**
 * Applies a schema to a DynamoDB table to make it accessible by a Hive query. DynamoDBDataFormat
 * is used with a HiveActivity object and a DynamoDBDataNode input and output. DynamoDBDataFormat
 * requires that you specify all columns in your Hive query.
 */
case class DynamoDBDataFormat private (
  id: PipelineObjectId,
  columns: Seq[String]
) extends DataFormat {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withColumns(col: String*) = this.copy(columns = columns ++ col)

  lazy val serialize = AdpDynamoDBDataFormat(
    id = id,
    name = id.toOption,
    column = columns
  )

}

object DynamoDBDataFormat {
  def apply() = new DynamoDBDataFormat(
    id = PipelineObjectId(DynamoDBDataFormat.getClass),
    columns = Seq()
  )
}
