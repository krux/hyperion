package com.krux.hyperion.dataformat

import com.krux.hyperion.aws.AdpCustomDataFormat
import com.krux.hyperion.common.PipelineObjectId

/**
 * A custom data format defined by a combination of a certain column separator, record separator,
 * and escape character.
 */
case class CustomDataFormat private (
  id: PipelineObjectId,
  columns: Seq[String],
  columnSeparator: String,
  recordSeparator: String
) extends DataFormat {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

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
    id = PipelineObjectId(CustomDataFormat.getClass),
    columns = Seq.empty,
    columnSeparator = ",",
    recordSeparator = "\n"
  )

}
