package com.krux.hyperion.h3.precondition

import com.krux.hyperion.adt.{ HDuration, HString }
import com.krux.hyperion.aws.AdpDynamoDBDataExistsPrecondition
import com.krux.hyperion.h3.common.{ ObjectFields, PipelineObjectId }
import com.krux.hyperion.HyperionContext

/**
 * A precondition to check that data exists in a DynamoDB table.
 *
 * @param tableName The DynamoDB table to check.
 */
case class DynamoDBDataExistsPrecondition private (
  baseFields: ObjectFields,
  preconditionFields: PreconditionFields,
  tableName: HString
) extends Precondition {

  type Self = DynamoDBDataExistsPrecondition

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
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
    baseFields = ObjectFields(PipelineObjectId(DynamoDBDataExistsPrecondition.getClass)),
    preconditionFields = Precondition.defaultPreconditionFields,
    tableName = tableName
  )

}
