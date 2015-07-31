package com.krux.hyperion.precondition

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.AdpDynamoDBTableExistsPrecondition
import com.krux.hyperion.common.PipelineObjectId

/**
 * A precondition to check that the DynamoDB table exists.
 *
 * @param tableName The DynamoDB table to check.
 */
case class DynamoDBTableExistsPrecondition private (
  id: PipelineObjectId,
  tableName: String,
  role: String,
  preconditionTimeout: Option[String]
) extends Precondition {

  def withPreconditionTimeout(timeout: String) = this.copy(preconditionTimeout = Option(timeout))
  def withRole(role: String) = this.copy(role = role)

  lazy val serialize = AdpDynamoDBTableExistsPrecondition(
    id = id,
    name = id.toOption,
    tableName = tableName,
    role = role,
    preconditionTimeout = preconditionTimeout
  )

}

object DynamoDBTableExistsPrecondition {
  def apply(tableName: String)(implicit hc: HyperionContext) =
    new DynamoDBTableExistsPrecondition(
      id = PipelineObjectId(DynamoDBTableExistsPrecondition.getClass),
      tableName = tableName,
      role = hc.role,
      preconditionTimeout = None
    )
}
