package com.krux.hyperion.h3.dataformat

import shapeless._

import com.krux.hyperion.adt.HString
import com.krux.hyperion.aws.AdpRegExDataFormat
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.h3.common.ObjectFields

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

  def baseFieldsLens = lens[Self] >> 'baseFields
  def dataFormatFieldsLens = lens[Self] >> 'dataFormatFields

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
