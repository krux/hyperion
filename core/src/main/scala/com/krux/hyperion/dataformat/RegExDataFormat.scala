package com.krux.hyperion.dataformat

import com.krux.hyperion.aws.AdpRegExDataFormat
import com.krux.hyperion.common.PipelineObjectId

/**
 * A custom data format defined by a regular expression.
 */
case class RegExDataFormat private (
  id: PipelineObjectId,
  inputRegEx: String,
  outputFormat: String,
  columns: Seq[String]
) extends DataFormat {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withColumns(col: String*) = this.copy(columns = columns ++ col)

  lazy val serialize = AdpRegExDataFormat(
    id = id,
    name = id.toOption,
    column = columns,
    inputRegEx = inputRegEx,
    outputFormat = outputFormat
  )

}

object RegExDataFormat {
  def apply(inputRegEx: String, outputFormat: String) = new RegExDataFormat(
    id = PipelineObjectId(RegExDataFormat.getClass),
    inputRegEx = inputRegEx,
    outputFormat = outputFormat,
    columns = Seq()
  )
}
