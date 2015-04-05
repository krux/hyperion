package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpRegExDataFormat

/**
 * RegEx data format
 */
case class RegExDataFormat private (
  id: PipelineObjectId,
  inputRegEx: String,
  outputFormat: String,
  column: Seq[String]
) extends DataFormat {

  def withColumns(cols: Seq[String]) = this.copy(column = cols)

  def serialize = AdpRegExDataFormat(
    id = id,
    name = Some(id),
    column = column match {
      case Seq() => None
      case columns => Some(columns)
    },
    inputRegEx = inputRegEx,
    outputFormat = outputFormat
  )

}

object RegExDataFormat {
  def apply(inputRegEx: String, outputFormat: String) = new RegExDataFormat(
    id = PipelineObjectId("RegExDataFormat"),
    inputRegEx = inputRegEx,
    outputFormat = outputFormat,
    column = Seq()
  )
}
