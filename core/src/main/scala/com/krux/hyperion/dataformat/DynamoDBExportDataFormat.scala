package com.krux.hyperion.dataformat

import com.krux.hyperion.aws.AdpDynamoDBExportDataFormat
import com.krux.hyperion.common.PipelineObjectId

/**
 * Applies a schema to an DynamoDB table to make it accessible by a Hive query. Use
 * DynamoDBExportDataFormat with a HiveCopyActivity object and DynamoDBDataNode or S3DataNode input
 * and output. DynamoDBExportDataFormat has the following benefits:
 *
 *   - Provides both DynamoDB and Amazon S3 support
 *
 *   - Allows you to filter data by certain columns in your Hive query
 *
 *   - Exports all attributes from DynamoDB even if you have a sparse schema
 */
case class DynamoDBExportDataFormat private (
  id: PipelineObjectId,
  columns: Seq[String]
) extends DataFormat {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withColumns(col: String*) = this.copy(columns = columns ++ col)

  lazy val serialize = AdpDynamoDBExportDataFormat(
    id = id,
    name = id.toOption,
    column = columns
  )

}

object DynamoDBExportDataFormat {
  def apply() = new DynamoDBExportDataFormat(
    id = PipelineObjectId(DynamoDBExportDataFormat.getClass),
    columns = Seq.empty
  )
}
