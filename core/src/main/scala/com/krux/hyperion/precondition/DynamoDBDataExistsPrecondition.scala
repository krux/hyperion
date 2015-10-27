package com.krux.hyperion.precondition

import com.krux.hyperion.adt.{HDuration, HString}
import com.krux.hyperion.aws.AdpDynamoDBDataExistsPrecondition
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.HyperionContext

/**
 * A precondition to check that data exists in a DynamoDB table.
 *
 * @param tableName The DynamoDB table to check.
 */
case class DynamoDBDataExistsPrecondition private (
  id: PipelineObjectId,
  tableName: HString,
  role: HString,
  preconditionTimeout: Option[HDuration]
) extends Precondition {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withRole(role: HString) = this.copy(role = role)
  def withPreconditionTimeout(timeout: HDuration) = this.copy(preconditionTimeout = Option(timeout))

  lazy val serialize = AdpDynamoDBDataExistsPrecondition(
    id = id,
    name = id.toOption,
    tableName = tableName.serialize,
    role = role.serialize,
    preconditionTimeout = preconditionTimeout.map(_.serialize)
  )

}

object DynamoDBDataExistsPrecondition {
  def apply(tableName: HString)(implicit hc: HyperionContext) =
    new DynamoDBDataExistsPrecondition(
      id = PipelineObjectId(DynamoDBDataExistsPrecondition.getClass),
      tableName = tableName,
      role = hc.role,
      preconditionTimeout = None
    )
}
