package com.krux.hyperion.precondition

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.AdpDynamoDBDataExistsPrecondition
import com.krux.hyperion.common.PipelineObjectId

/**
 * A precondition to check that data exists in a DynamoDB table.
 *
 * @param tableName The DynamoDB table to check.
 */
case class DynamoDBDataExistsPrecondition private (
  id: PipelineObjectId,
  tableName: String,
  preconditionTimeout: Option[String],
  role: String
) extends Precondition {

  def withPreconditionTimeout(timeout: String) = this.copy(preconditionTimeout = Option(timeout))
  def withRole(role: String) = this.copy(role = role)

  lazy val serialize = AdpDynamoDBDataExistsPrecondition(
    id = id,
    name = id.toOption,
    preconditionTimeout = preconditionTimeout,
    role = role,
    tableName = tableName
  )

}

object DynamoDBDataExistsPrecondition {
  def apply(tableName: String)(implicit hc: HyperionContext) =
    new DynamoDBDataExistsPrecondition(
      id = PipelineObjectId("DynamoDBDataExistsPrecondition"),
      tableName = tableName,
      preconditionTimeout = None,
      role = hc.role
    )
}
