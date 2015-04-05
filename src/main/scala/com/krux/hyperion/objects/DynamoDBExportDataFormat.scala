package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpDynamoDBExportDataFormat

/**
 * DynamoDB Export data format
 */
case class DynamoDBExportDataFormat private (
  id: PipelineObjectId,
  column: Seq[String]
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
  def apply() =
    new DynamoDBExportDataFormat(
      id = PipelineObjectId("DynamoDBExportDataFormat"),
      column = Seq()
    )
}
