package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpTsvDataFormat, AdpJsonSerializer}
import com.krux.hyperion.util.PipelineId

/**
 * TSV data format
 */
case class TsvDataFormat (
  id: String,
  column: Option[Seq[String]] = None,
  escapeChar: Option[String] = None
) extends DataFormat {

  def withColumns(cols: Seq[String]) = this.copy(column = Some(cols))

  def withEscapeChar(escapeChar: String) = this.copy(escapeChar = Option(escapeChar))

  def serialize = AdpTsvDataFormat(
    id = id,
    name = Some(id),
    column = column,
    escapeChar = escapeChar
  )

}

object TsvDataFormat {
  def apply() = new TsvDataFormat(PipelineId.generateNewId("TsvDataFormat"))
}
