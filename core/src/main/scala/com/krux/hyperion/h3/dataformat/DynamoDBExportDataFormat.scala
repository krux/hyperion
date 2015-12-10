package com.krux.hyperion.h3.dataformat

import com.krux.hyperion.adt.HString
import com.krux.hyperion.aws.AdpDynamoDBExportDataFormat
import com.krux.hyperion.h3.common.{ ObjectFields, PipelineObjectId }

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
  baseFields: ObjectFields,
  dataFormatFields: DataFormatFields
) extends DataFormat {

  type Self = DynamoDBExportDataFormat

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateDataFormatFields(fields: DataFormatFields) = copy(dataFormatFields = fields)

  lazy val serialize = AdpDynamoDBExportDataFormat(
    id = id,
    name = id.toOption,
    column = columns.map(_.serialize)
  )

}

object DynamoDBExportDataFormat {

  def apply() = new DynamoDBExportDataFormat(
    baseFields = ObjectFields(PipelineObjectId(CsvDataFormat.getClass)),
    dataFormatFields = DataFormatFields()
  )

}
