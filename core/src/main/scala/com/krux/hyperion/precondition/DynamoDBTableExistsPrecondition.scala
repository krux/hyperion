package com.krux.hyperion.precondition

import com.krux.hyperion.adt.{HDuration, HString}
import com.krux.hyperion.aws.AdpDynamoDBTableExistsPrecondition
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.HyperionContext

/**
 * A precondition to check that the DynamoDB table exists.
 *
 * @param tableName The DynamoDB table to check.
 */
case class DynamoDBTableExistsPrecondition private (
  id: PipelineObjectId,
  tableName: HString,
  role: HString,
  preconditionTimeout: Option[HDuration]
) extends Precondition {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withRole(role: HString) = this.copy(role = role)
  def withPreconditionTimeout(timeout: HDuration) = this.copy(preconditionTimeout = Option(timeout))

  lazy val serialize = AdpDynamoDBTableExistsPrecondition(
    id = id,
    name = id.toOption,
    tableName = tableName.serialize,
    role = role.serialize,
    preconditionTimeout = preconditionTimeout.map(_.serialize)
  )

}

object DynamoDBTableExistsPrecondition {
  def apply(tableName: HString)(implicit hc: HyperionContext) =
    new DynamoDBTableExistsPrecondition(
      id = PipelineObjectId(DynamoDBTableExistsPrecondition.getClass),
      tableName = tableName,
      role = hc.role,
      preconditionTimeout = None
    )
}
