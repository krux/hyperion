package com.krux.hyperion.objects.dataformat

import com.krux.hyperion.objects.PipelineObjectId
import com.krux.hyperion.objects.aws.AdpTsvDataFormat

/**
 * A comma-delimited data format where the column separator is a tab character and the record
 * separator is a newline character.
 */
case class TsvDataFormat private (
  id: PipelineObjectId,
  columns: Seq[String] = Seq(),
  escapeChar: Option[String] = None
) extends DataFormat {

  def withColumns(col: String*) = this.copy(columns = columns ++ col)

  def withEscapeChar(escapeChar: String) = this.copy(escapeChar = Option(escapeChar))

  lazy val serialize = AdpTsvDataFormat(
    id = id,
    name = id.toOption,
    column = columns,
    escapeChar = escapeChar
  )

}

object TsvDataFormat {

  def apply() = new TsvDataFormat(PipelineObjectId("TsvDataFormat"))

}
