package com.krux.hyperion.h3.dataformat

import shapeless._

import com.krux.hyperion.adt.HString
import com.krux.hyperion.aws.AdpCustomDataFormat
import com.krux.hyperion.h3.common.{ ObjectFields, PipelineObjectId }

/**
 * A custom data format defined by a combination of a certain column separator, record separator,
 * and escape character.
 */
case class CustomDataFormat private (
  baseFields: ObjectFields,
  dataFormatFields: DataFormatFields,
  columnSeparator: HString,
  recordSeparator: HString
) extends DataFormat {

  type Self = CustomDataFormat

  def baseFieldsLens = lens[Self] >> 'baseFields
  def dataFormatFieldsLens = lens[Self] >> 'dataFormatFields

  def withColumnSeparator(columnSeparator: HString) = this.copy(columnSeparator = columnSeparator)
  def withRecordSeparator(recordSeparator: HString) = this.copy(recordSeparator = recordSeparator)

  lazy val serialize = AdpCustomDataFormat(
    id = id,
    name = id.toOption,
    column = columns.map(_.serialize),
    columnSeparator = columnSeparator.serialize,
    recordSeparator = recordSeparator.serialize
  )

}

object CustomDataFormat {

  def apply() = new CustomDataFormat(
    baseFields = ObjectFields(PipelineObjectId(CsvDataFormat.getClass)),
    dataFormatFields = DataFormatFields(),
    columnSeparator = ",",
    recordSeparator = "\n"
  )

}
