package com.krux.hyperion.precondition

import com.krux.hyperion.aws.AdpExistsPrecondition
import com.krux.hyperion.common.{ PipelineObjectId, BaseFields }
import com.krux.hyperion.HyperionContext

/**
 * Checks whether a data node object exists.
 */
case class ExistsPrecondition private (
  baseFields: BaseFields,
  preconditionFields: PreconditionFields
) extends Precondition {

  type Self = ExistsPrecondition

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updatePreconditionFields(fields: PreconditionFields) = copy(preconditionFields = fields)

  lazy val serialize = AdpExistsPrecondition(
    id = id,
    name = id.toOption,
    role = role.serialize,
    preconditionTimeout = preconditionTimeout.map(_.serialize)
  )

}

object ExistsPrecondition {

  def apply()(implicit hc: HyperionContext) = new ExistsPrecondition(
    baseFields = BaseFields(PipelineObjectId(ExistsPrecondition.getClass)),
    preconditionFields = Precondition.defaultPreconditionFields
  )

}
