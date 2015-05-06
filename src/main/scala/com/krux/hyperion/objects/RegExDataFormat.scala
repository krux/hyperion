package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpRegExDataFormat

/**
 * RegEx data format
 */
case class RegExDataFormat private (
  id: PipelineObjectId,
  inputRegEx: String,
  outputFormat: String,
  columns: Seq[String]
) extends DataFormat {

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
    id = PipelineObjectId("RegExDataFormat"),
    inputRegEx = inputRegEx,
    outputFormat = outputFormat,
    columns = Seq()
  )
}
