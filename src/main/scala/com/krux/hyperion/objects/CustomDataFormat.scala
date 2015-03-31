package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpCustomDataFormat
import com.krux.hyperion.util.PipelineId

/**
 * Custom data format
 */
case class CustomDataFormat (
  id: String,
  column: Seq[String] = Seq(),
  columnSeparator: String = ",",
  recordSeparator: String = "\n"
) extends DataFormat {

  def withColumns(cols: Seq[String]) = this.copy(column = cols)
  def withColumnSeparator(columnSeparator: String) = this.copy(columnSeparator = columnSeparator)
  def withRecordSeparator(recordSeparator: String) = this.copy(recordSeparator = recordSeparator)

  def serialize = AdpCustomDataFormat(
    id = id,
    name = Some(id),
    column = column match {
      case Seq() => None
      case columns => Some(columns)
    },
    columnSeparator = columnSeparator,
    recordSeparator = recordSeparator
  )

}

object CustomDataFormat {
  def apply() = new CustomDataFormat(PipelineId.generateNewId("CustomDataFormat"))
}
