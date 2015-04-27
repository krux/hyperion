package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpDynamoDBDataFormat

/**
 * DynamoDB data format
 */
case class DynamoDBDataFormat private (
  id: PipelineObjectId,
  column: Seq[String]
) extends DataFormat {

  def withColumns(cols: Seq[String]) = this.copy(column = cols)

  lazy val serialize = AdpDynamoDBDataFormat(
    id = id,
    name = id.toOption,
    column = column
  )

}

object DynamoDBDataFormat {
  def apply() =
    new DynamoDBDataFormat(
      id = PipelineObjectId("DynamoDBDataFormat"),
      column = Seq()
    )
}
