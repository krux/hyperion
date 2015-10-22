package com.krux.hyperion.precondition

import com.krux.hyperion.aws.AdpExistsPrecondition
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.adt.HDuration
import com.krux.hyperion.HyperionContext

/**
 * Checks whether a data node object exists.
 */
case class ExistsPrecondition private (
  id: PipelineObjectId,
  role: String,
  preconditionTimeout: Option[HDuration]
) extends Precondition {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withRole(role: String) = this.copy(role = role)
  def withPreconditionTimeout(timeout: HDuration) = this.copy(preconditionTimeout = Option(timeout))

  lazy val serialize = AdpExistsPrecondition(
    id = id,
    name = id.toOption,
    role = role,
    preconditionTimeout = preconditionTimeout.map(_.toString)
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
