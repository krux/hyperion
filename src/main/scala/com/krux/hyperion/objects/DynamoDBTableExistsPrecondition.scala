package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpDynamoDBTableExistsPrecondition

/**
 * A precondition to check that the DynamoDB table exists.
 *
 * @param tableName The DynamoDB table to check.
 */
case class DynamoDBTableExistsPrecondition private (
  id: UniquePipelineId,
  tableName: String,
  preconditionTimeout: Option[String],
  role: Option[String]
)(
  implicit val hc: HyperionContext
) extends Precondition {

  def serialize = AdpDynamoDBTableExistsPrecondition(
    id = id,
    name = Some(id),
    preconditionTimeout = preconditionTimeout,
    role = role.getOrElse(hc.resourceRole),
    tableName = tableName
  )

}

object DynamoDBTableExistsPrecondition {
  def apply(tableName: String)(implicit hc: HyperionContext) =
    new DynamoDBTableExistsPrecondition(
      id = new UniquePipelineId("DynamoDBTableExistsPrecondition"),
      tableName = tableName,
      preconditionTimeout = None,
      role = None
    )
}
