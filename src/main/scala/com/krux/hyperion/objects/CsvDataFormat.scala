package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpCsvDataFormat, AdpJsonSerializer}
import com.krux.hyperion.util.PipelineId

/**
 * CSV data format
 */
case class CsvDataFormat private (
    id: String,
    column: Option[Seq[String]] = None
  ) extends DataFormat {

  def withColumns(cols: Seq[String]) = this.copy(column = Some(cols))
  def serialize = AdpCsvDataFormat(id, Some(id), column, None)

}

object CsvDataFormat {
  def apply() = new CsvDataFormat(PipelineId.generateNewId("CsvDataFormat"))
}
