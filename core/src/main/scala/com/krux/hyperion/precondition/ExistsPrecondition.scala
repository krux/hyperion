package com.krux.hyperion.precondition

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.AdpExistsPrecondition
import com.krux.hyperion.common.PipelineObjectId

/**
 * Checks whether a data node object exists.
 */
case class ExistsPrecondition private (
  id: PipelineObjectId,
  role: String,
  preconditionTimeout: Option[String]
) extends Precondition {

  def withPreconditionTimeout(timeout: String) = this.copy(preconditionTimeout = Option(timeout))
  def withRole(role: String) = this.copy(role = role)

  lazy val serialize = AdpExistsPrecondition(
    id = id,
    name = id.toOption,
    role = role,
    preconditionTimeout = preconditionTimeout
  )

}

object ExistsPrecondition {
  def apply()(implicit hc: HyperionContext) =
    new ExistsPrecondition(
      id = PipelineObjectId(ExistsPrecondition.getClass),
      role = hc.role,
      preconditionTimeout = None
    )
}
