package com.krux.hyperion.precondition

import com.krux.hyperion.adt.{HDuration, HString}
import com.krux.hyperion.aws.AdpExistsPrecondition
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.HyperionContext

/**
 * Checks whether a data node object exists.
 */
case class ExistsPrecondition private (
  id: PipelineObjectId,
  role: HString,
  preconditionTimeout: Option[HDuration]
) extends Precondition {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withRole(role: HString) = this.copy(role = role)
  def withPreconditionTimeout(timeout: HDuration) = this.copy(preconditionTimeout = Option(timeout))

  lazy val serialize = AdpExistsPrecondition(
    id = id,
    name = id.toOption,
    role = role.serialize,
    preconditionTimeout = preconditionTimeout.map(_.serialize)
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
