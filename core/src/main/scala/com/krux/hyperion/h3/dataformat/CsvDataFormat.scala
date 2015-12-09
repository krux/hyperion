package com.krux.hyperion.h3.dataformat

import shapeless._

import com.krux.hyperion.adt.HString
import com.krux.hyperion.aws.AdpCsvDataFormat
import com.krux.hyperion.h3.common.{ ObjectFields, PipelineObjectId }

/**
 * A comma-delimited data format where the column separator is a comma and the record separator is
 * a newline character.
 */
case class CsvDataFormat private (
  baseFields: ObjectFields,
  dataFormatFields: DataFormatFields,
  escapeChar: Option[HString]
) extends DataFormat {

  type Self = CsvDataFormat

  def baseFieldsLens = lens[Self] >> 'baseFields
  def dataFormatFieldsLens = lens[Self] >> 'dataFormatFields

  def withEscapeChar(escapeChar: HString) = this.copy(escapeChar = Option(escapeChar))

  lazy val serialize = AdpCsvDataFormat(
    id = id,
    name = id.toOption,
    column = columns.map(_.serialize),
    escapeChar = escapeChar.map(_.serialize)
  )

}

object CsvDataFormat {

  def apply() = new CsvDataFormat(
    baseFields = ObjectFields(PipelineObjectId(CsvDataFormat.getClass)),
    dataFormatFields = DataFormatFields(),
    escapeChar = None
  )

}
