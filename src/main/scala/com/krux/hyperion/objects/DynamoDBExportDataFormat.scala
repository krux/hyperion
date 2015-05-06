package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpDynamoDBExportDataFormat

/**
 * DynamoDB Export data format
 */
case class DynamoDBExportDataFormat private (
  id: PipelineObjectId,
  columns: Seq[String]
) extends DataFormat {

  def withColumns(col: String*) = this.copy(columns = columns ++ col)

  lazy val serialize = AdpDynamoDBExportDataFormat(
    id = id,
    name = id.toOption,
    column = columns
  )

}

object DynamoDBExportDataFormat {
  def apply() =
    new DynamoDBExportDataFormat(
      id = PipelineObjectId("DynamoDBExportDataFormat"),
      columns = Seq()
    )
}
