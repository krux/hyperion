package com.krux.hyperion.h3.dataformat

import shapeless._

import com.krux.hyperion.adt.HString
import com.krux.hyperion.aws.AdpDynamoDBDataFormat
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.h3.common.ObjectFields

/**
 * Applies a schema to a DynamoDB table to make it accessible by a Hive query. DynamoDBDataFormat
 * is used with a HiveActivity object and a DynamoDBDataNode input and output. DynamoDBDataFormat
 * requires that you specify all columns in your Hive query.
 */
case class DynamoDBDataFormat private (
  baseFields: ObjectFields,
  dataFormatFields: DataFormatFields
) extends DataFormat {

  type Self = DynamoDBDataFormat

  def baseFieldsLens = lens[Self] >> 'baseFields
  def dataFormatFieldsLens = lens[Self] >> 'dataFormatFields

  lazy val serialize = AdpDynamoDBDataFormat(
    id = id,
    name = id.toOption,
    column = columns.map(_.serialize)
  )

}

object DynamoDBDataFormat {

  def apply() = new DynamoDBDataFormat(
    baseFields = ObjectFields(PipelineObjectId(CsvDataFormat.getClass)),
    dataFormatFields = DataFormatFields()
  )

}
