package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpCustomDataFormat

/**
 * Custom data format
 */
case class CustomDataFormat private (
  id: PipelineObjectId,
  column: Seq[String],
  columnSeparator: String,
  recordSeparator: String
) extends DataFormat {

  def withColumns(cols: Seq[String]) = this.copy(column = cols)
  def withColumnSeparator(columnSeparator: String) = this.copy(columnSeparator = columnSeparator)
  def withRecordSeparator(recordSeparator: String) = this.copy(recordSeparator = recordSeparator)

  lazy val serialize = AdpCustomDataFormat(
    id = id,
    name = id.toOption,
    column = column,
    columnSeparator = columnSeparator,
    recordSeparator = recordSeparator
  )

}

object CustomDataFormat {
  def apply() = new CustomDataFormat(
    id = PipelineObjectId("CustomDataFormat"),
    column = Seq(),
    columnSeparator = ",",
    recordSeparator = "\n"
  )
}
