package com.krux.hyperion.precondition

import com.krux.hyperion.adt.HString
import com.krux.hyperion.aws.AdpDynamoDBDataExistsPrecondition
import com.krux.hyperion.common.{ BaseFields, PipelineObjectId }
import com.krux.hyperion.HyperionContext

/**
 * A precondition to check that data exists in a DynamoDB table.
 *
 * @param tableName The DynamoDB table to check.
 */
case class DynamoDBDataExistsPrecondition private (
  baseFields: BaseFields,
  preconditionFields: PreconditionFields,
  tableName: HString
) extends Precondition {

  type Self = DynamoDBDataExistsPrecondition

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updatePreconditionFields(fields: PreconditionFields) = copy(preconditionFields = fields)

  lazy val serialize = AdpDynamoDBDataExistsPrecondition(
    id = id,
    name = id.toOption,
    tableName = tableName.serialize,
    role = role.serialize,
    preconditionTimeout = preconditionTimeout.map(_.serialize)
  )

}

object DynamoDBDataExistsPrecondition {

  def apply(tableName: HString)(implicit hc: HyperionContext) = new DynamoDBDataExistsPrecondition(
    baseFields = BaseFields(PipelineObjectId(DynamoDBDataExistsPrecondition.getClass)),
    preconditionFields = Precondition.defaultPreconditionFields,
    tableName = tableName
  )

}
