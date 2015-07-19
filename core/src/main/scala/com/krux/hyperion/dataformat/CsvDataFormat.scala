package com.krux.hyperion.dataformat

import com.krux.hyperion.aws.AdpCsvDataFormat
import com.krux.hyperion.common.PipelineObjectId

/**
 * A comma-delimited data format where the column separator is a comma and the record separator is
 * a newline character.
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
