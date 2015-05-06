package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpDynamoDBDataFormat

/**
 * DynamoDB data format
 */
case class DynamoDBDataFormat private (
  id: PipelineObjectId,
  columns: Seq[String]
) extends DataFormat {

  def withColumns(col: String*) = this.copy(columns = columns ++ col)

  lazy val serialize = AdpDynamoDBDataFormat(
    id = id,
    name = id.toOption,
    column = columns
  )

}

object DynamoDBDataFormat {
  def apply() =
    new DynamoDBDataFormat(
      id = PipelineObjectId("DynamoDBDataFormat"),
      columns = Seq()
    )
}
