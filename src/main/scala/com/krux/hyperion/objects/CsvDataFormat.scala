package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpCsvDataFormat, AdpJsonSerializer}
import com.krux.hyperion.util.PipelineId

/**
 * CSV data format
 */
case class CsvDataFormat (
  id: String,
  column: Option[Seq[String]] = None,
  escapeChar: Option[String] = None
) extends DataFormat {

  def withColumns(cols: Seq[String]) = this.copy(column = Some(cols))

  def withEscapeChar(escapeChar: String) = this.copy(escapeChar = Option(escapeChar))

  def serialize = AdpCsvDataFormat(
    id = id,
    name = Some(id),
    column = column,
    escapeChar = None
  )

}

object CsvDataFormat {
  def apply() = new CsvDataFormat(PipelineId.generateNewId("CsvDataFormat"))
}
