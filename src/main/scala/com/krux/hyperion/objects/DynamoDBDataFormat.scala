package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpDynamoDBDataFormat
import com.krux.hyperion.util.PipelineId

/**
 * DynamoDB data format
 */
case class DynamoDBDataFormat (
  id: String,
  column: Seq[String] = Seq()
) extends DataFormat {

  def withColumns(cols: Seq[String]) = this.copy(column = cols)

  def serialize = AdpDynamoDBDataFormat(
    id = id,
    name = Some(id),
    column = column match {
      case Seq() => None
      case columns => Some(columns)
    }
  )

}

object DynamoDBDataFormat {
  def apply() = new DynamoDBDataFormat(PipelineId.generateNewId("DynamoDBDataFormat"))
}
