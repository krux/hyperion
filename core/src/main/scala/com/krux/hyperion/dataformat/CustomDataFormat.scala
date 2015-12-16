package com.krux.hyperion.dataformat

import com.krux.hyperion.adt.HString
import com.krux.hyperion.aws.AdpCustomDataFormat
import com.krux.hyperion.common.{ BaseFields, PipelineObjectId }

/**
 * A custom data format defined by a combination of a certain column separator, record separator,
 * and escape character.
 */
case class CustomDataFormat private (
  baseFields: BaseFields,
  dataFormatFields: DataFormatFields,
  columnSeparator: HString,
  recordSeparator: HString
) extends DataFormat {

  type Self = CustomDataFormat

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateDataFormatFields(fields: DataFormatFields) = copy(dataFormatFields = fields)

  def withColumnSeparator(columnSeparator: HString) = copy(columnSeparator = columnSeparator)
  def withRecordSeparator(recordSeparator: HString) = copy(recordSeparator = recordSeparator)

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
    baseFields = BaseFields(PipelineObjectId(CsvDataFormat.getClass)),
    dataFormatFields = DataFormatFields(),
    columnSeparator = ",",
    recordSeparator = "\n"
  )

}
