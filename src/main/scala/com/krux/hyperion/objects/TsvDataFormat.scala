package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpTsvDataFormat, AdpJsonSerializer}
import com.krux.hyperion.util.PipelineId

/**
 * TSV data format
 */
case class TsvDataFormat private (
    id: String,
    column: Option[Seq[String]] = None
  ) extends DataFormat {

  def withColumns(cols: Seq[String]) = this.copy(column = Some(cols))
  def serialize = AdpTsvDataFormat(id, Some(id), column, None)

}

object TsvDataFormat {
  def apply() = new TsvDataFormat(PipelineId.generateNewId("TsvDataFormat"))
}
