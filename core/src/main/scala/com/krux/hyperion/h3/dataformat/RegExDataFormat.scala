package com.krux.hyperion.h3.dataformat

import com.krux.hyperion.adt.HString
import com.krux.hyperion.aws.AdpRegExDataFormat
import com.krux.hyperion.h3.common.{ ObjectFields, PipelineObjectId }

/**
 * A custom data format defined by a regular expression.
 */
case class RegExDataFormat private (
  baseFields: ObjectFields,
  dataFormatFields: DataFormatFields,
  inputRegEx: HString,
  outputFormat: HString
) extends DataFormat {

  type Self = RegExDataFormat

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateDataFormatFields(fields: DataFormatFields) = copy(dataFormatFields = fields)

  lazy val serialize = AdpRegExDataFormat(
    id = id,
    name = id.toOption,
    column = columns.map(_.serialize),
    inputRegEx = inputRegEx.serialize,
    outputFormat = outputFormat.serialize
  )

}

object RegExDataFormat {

  def apply(inputRegEx: HString, outputFormat: HString) = new RegExDataFormat(
    baseFields = ObjectFields(PipelineObjectId(CsvDataFormat.getClass)),
    dataFormatFields = DataFormatFields(),
    inputRegEx = inputRegEx,
    outputFormat = outputFormat
  )

}
