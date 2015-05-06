package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpCsvDataFormat

/**
 * CSV data format
 */
case class CsvDataFormat private (
  id: PipelineObjectId,
  columns: Seq[String],
  escapeChar: Option[String]
) extends DataFormat {

  def withColumns(col: String*) = this.copy(columns = columns ++ col)
  def withEscapeChar(escapeChar: String) = this.copy(escapeChar = Option(escapeChar))

  lazy val serialize = AdpCsvDataFormat(
    id = id,
    name = id.toOption,
    column = columns,
    escapeChar = None
  )

}

object CsvDataFormat {
  def apply() = new CsvDataFormat(
    id = PipelineObjectId("CsvDataFormat"),
    columns = Seq(),
    escapeChar = None
  )
}
