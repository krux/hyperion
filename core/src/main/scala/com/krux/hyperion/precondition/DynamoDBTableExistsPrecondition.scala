package com.krux.hyperion.precondition

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.AdpDynamoDBTableExistsPrecondition
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.expression.DpPeriod

/**
 * A precondition to check that the DynamoDB table exists.
 *
 * @param tableName The DynamoDB table to check.
 */
case class DynamoDBTableExistsPrecondition private (
  id: PipelineObjectId,
  tableName: String,
  role: String,
  preconditionTimeout: Option[DpPeriod]
) extends Precondition {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withRole(role: String) = this.copy(role = role)
  def withPreconditionTimeout(timeout: DpPeriod) = this.copy(preconditionTimeout = Option(timeout))

  lazy val serialize = AdpDynamoDBTableExistsPrecondition(
    id = id,
    name = id.toOption,
    tableName = tableName,
    role = role,
    preconditionTimeout = preconditionTimeout.map(_.toString)
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
