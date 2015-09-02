package com.krux.hyperion.dataformat

import com.krux.hyperion.aws.AdpTsvDataFormat
import com.krux.hyperion.common.PipelineObjectId

/**
 * A tab-delimited data format where the record separator is a newline character.
 */
case class TsvDataFormat private (
  id: PipelineObjectId,
  columns: Seq[String] = Seq.empty,
  escapeChar: Option[String] = None
) extends DataFormat {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

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

  def apply() = new TsvDataFormat(PipelineObjectId(TsvDataFormat.getClass))

}
