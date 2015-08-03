package com.krux.hyperion.precondition

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.AdpExistsPrecondition
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.expression.Duration
import com.krux.hyperion.parameter.Parameter

/**
 * Checks whether a data node object exists.
 */
case class ExistsPrecondition private (
  id: PipelineObjectId,
  role: String,
  preconditionTimeout: Option[Parameter[Duration]]
) extends Precondition {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withRole(role: String) = this.copy(role = role)
  def withPreconditionTimeout(timeout: Parameter[Duration]) = this.copy(preconditionTimeout = Option(timeout))

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
