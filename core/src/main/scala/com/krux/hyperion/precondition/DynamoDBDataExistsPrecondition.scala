package com.krux.hyperion.precondition

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.AdpDynamoDBDataExistsPrecondition
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.expression.Duration
import com.krux.hyperion.parameter.Parameter

/**
 * A precondition to check that data exists in a DynamoDB table.
 *
 * @param tableName The DynamoDB table to check.
 */
case class DynamoDBDataExistsPrecondition private (
  id: PipelineObjectId,
  tableName: String,
  role: String,
  preconditionTimeout: Option[Parameter[Duration]]
) extends Precondition {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withRole(role: String) = this.copy(role = role)
  def withPreconditionTimeout(timeout: Parameter[Duration]) = this.copy(preconditionTimeout = Option(timeout))

  lazy val serialize = AdpDynamoDBDataExistsPrecondition(
    id = id,
    name = id.toOption,
    tableName = tableName,
    role = role,
    preconditionTimeout = preconditionTimeout.map(_.toString)
  )

}

object DynamoDBDataExistsPrecondition {
  def apply(tableName: String)(implicit hc: HyperionContext) =
    new DynamoDBDataExistsPrecondition(
      id = PipelineObjectId(DynamoDBDataExistsPrecondition.getClass),
      tableName = tableName,
      role = hc.role,
      preconditionTimeout = None
    )
}
