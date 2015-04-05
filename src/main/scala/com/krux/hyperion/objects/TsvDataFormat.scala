package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpTsvDataFormat

/**
 * TSV data format
 */
case class TsvDataFormat private (
  id: PipelineObjectId,
  column: Seq[String] = Seq(),
  escapeChar: Option[String] = None
) extends DataFormat {

  def withColumns(cols: Seq[String]) = this.copy(column = cols)

  def withEscapeChar(escapeChar: String) = this.copy(escapeChar = Option(escapeChar))

  def serialize = AdpTsvDataFormat(
    id = id,
    name = Some(id),
    column = column match {
      case Seq() => None
      case columns => Some(columns)
    },
    escapeChar = escapeChar
  )

}

object TsvDataFormat {
  def apply() = new TsvDataFormat(PipelineObjectId("TsvDataFormat"))
}
