package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpCustomDataFormat

/**
 * Custom data format
 */
case class CustomDataFormat private (
  id: PipelineObjectId,
  columns: Seq[String],
  columnSeparator: String,
  recordSeparator: String
) extends DataFormat {

  def withColumns(col: String*) = this.copy(columns = columns ++ col)
  def withColumnSeparator(columnSeparator: String) = this.copy(columnSeparator = columnSeparator)
  def withRecordSeparator(recordSeparator: String) = this.copy(recordSeparator = recordSeparator)

  lazy val serialize = AdpCustomDataFormat(
    id = id,
    name = id.toOption,
    column = columns,
    columnSeparator = columnSeparator,
    recordSeparator = recordSeparator
  )

}

object CustomDataFormat {
  def apply() = new CustomDataFormat(
    id = PipelineObjectId("CustomDataFormat"),
    columns = Seq(),
    columnSeparator = ",",
    recordSeparator = "\n"
  )
}
