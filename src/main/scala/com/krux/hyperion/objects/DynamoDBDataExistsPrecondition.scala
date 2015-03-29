package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpDynamoDBDataExistsPrecondition

/**
 * A precondition to check that data exists in a DynamoDB table.
 *
 * @param tableName The DynamoDB table to check.
 */
case class DynamoDBDataExistsPrecondition (
  id: String,
  tableName: String,
  preconditionTimeout: Option[String] = None,
  role: Option[String] = None
)(
  implicit val hc: HyperionContext
) extends Precondition {

  def serialize = AdpDynamoDBDataExistsPrecondition(
    id = id,
    name = Some(id),
    preconditionTimeout = preconditionTimeout,
    role = role.getOrElse(hc.resourceRole),
    tableName = tableName
  )

}
