package com.krux.hyperion.precondition

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.AdpExistsPrecondition
import com.krux.hyperion.common.PipelineObjectId

/**
 * Checks whether a data node object exists.
 */
case class ExistsPrecondition private (
  id: PipelineObjectId,
  preconditionTimeout: Option[String],
  role: String
) extends Precondition {

  def withPreconditionTimeout(timeout: String) = this.copy(preconditionTimeout = Option(timeout))
  def withRole(role: String) = this.copy(role = role)

  lazy val serialize = AdpExistsPrecondition(
    id = id,
    name = id.toOption,
    preconditionTimeout = preconditionTimeout,
    role = role
  )

}

object ExistsPrecondition {
  def apply()(implicit hc: HyperionContext) =
    new ExistsPrecondition(
      id = PipelineObjectId("ExistsPrecondition"),
      preconditionTimeout = None,
      role = hc.role
    )
}
