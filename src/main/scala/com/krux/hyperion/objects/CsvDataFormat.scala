package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpCsvDataFormat

/**
 * CSV data format
 */
case class CsvDataFormat private (
  id: PipelineObjectId,
  column: Seq[String],
  escapeChar: Option[String]
) extends DataFormat {

  def withColumns(cols: Seq[String]) = this.copy(column = cols)

  def withEscapeChar(escapeChar: String) = this.copy(escapeChar = Option(escapeChar))

  lazy val serialize = AdpCsvDataFormat(
    id = id,
    name = id.toOption,
    column = column,
    escapeChar = None
  )

}

object CsvDataFormat {
  def apply() = new CsvDataFormat(
    id = PipelineObjectId("CsvDataFormat"),
    column = Seq(),
    escapeChar = None
  )
}
