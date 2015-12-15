package com.krux.hyperion.dataformat

import com.krux.hyperion.adt.HString
import com.krux.hyperion.aws.AdpTsvDataFormat
import com.krux.hyperion.common.{ ObjectFields, PipelineObjectId }

/**
 * A tab-delimited data format where the record separator is a newline character.
 */
case class TsvDataFormat private (
  baseFields: ObjectFields,
  dataFormatFields: DataFormatFields,
  escapeChar: Option[HString]
) extends DataFormat {

  type Self = TsvDataFormat

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateDataFormatFields(fields: DataFormatFields) = copy(dataFormatFields = fields)

  def withEscapeChar(escapeChar: HString) = this.copy(escapeChar = Option(escapeChar))

  lazy val serialize = AdpTsvDataFormat(
    id = id,
    name = id.toOption,
    column = columns.map(_.serialize),
    escapeChar = escapeChar.map(_.serialize)
  )

}

object TsvDataFormat {

  def apply() = new TsvDataFormat(
    baseFields = ObjectFields(PipelineObjectId(TsvDataFormat.getClass)),
    dataFormatFields = DataFormatFields(),
    escapeChar = None
  )

}
