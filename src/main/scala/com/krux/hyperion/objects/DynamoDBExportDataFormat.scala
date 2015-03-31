package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpDynamoDBExportDataFormat
import com.krux.hyperion.util.PipelineId

/**
 * DynamoDB Export data format
 */
case class DynamoDBExportDataFormat (
  id: String,
  column: Seq[String] = Seq()
) extends DataFormat {

  def withColumns(cols: Seq[String]) = this.copy(column = cols)

  def serialize = AdpDynamoDBExportDataFormat(
    id = id,
    name = Some(id),
    column = column match {
      case Seq() => None
      case columns => Some(columns)
    }
  )

}

object DynamoDBExportDataFormat {
  def apply() = new DynamoDBExportDataFormat(PipelineId.generateNewId("DynamoDBExportDataFormat"))
}
