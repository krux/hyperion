package com.krux.hyperion.h3.precondition

import com.krux.hyperion.adt.{ HDuration, HString }
import com.krux.hyperion.aws.AdpDynamoDBTableExistsPrecondition
import com.krux.hyperion.h3.common.{ ObjectFields, PipelineObjectId }
import com.krux.hyperion.HyperionContext

/**
 * A precondition to check that the DynamoDB table exists.
 *
 * @param tableName The DynamoDB table to check.
 */
case class DynamoDBTableExistsPrecondition private (
  baseFields: ObjectFields,
  preconditionFields: PreconditionFields,
  tableName: HString
) extends Precondition {

  type Self = DynamoDBTableExistsPrecondition

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updatePreconditionFields(fields: PreconditionFields) = copy(preconditionFields = fields)

  lazy val serialize = AdpDynamoDBTableExistsPrecondition(
    id = id,
    name = id.toOption,
    tableName = tableName.serialize,
    role = role.serialize,
    preconditionTimeout = preconditionTimeout.map(_.serialize)
  )

}

object DynamoDBTableExistsPrecondition {

  def apply(tableName: HString)(implicit hc: HyperionContext) = new DynamoDBTableExistsPrecondition(
    baseFields = ObjectFields(PipelineObjectId(DynamoDBTableExistsPrecondition.getClass)),
    preconditionFields = Precondition.defaultPreconditionFields,
    tableName = tableName
  )

}
