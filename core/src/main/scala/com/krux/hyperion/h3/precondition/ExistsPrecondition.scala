package com.krux.hyperion.h3.precondition

import com.krux.hyperion.adt.{ HDuration, HString }
import com.krux.hyperion.aws.AdpExistsPrecondition
import com.krux.hyperion.h3.common.{ PipelineObjectId, ObjectFields }
import com.krux.hyperion.HyperionContext

/**
 * Checks whether a data node object exists.
 */
case class ExistsPrecondition private (
  baseFields: ObjectFields,
  preconditionFields: PreconditionFields
) extends Precondition {

  type Self = ExistsPrecondition

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
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
    baseFields = ObjectFields(PipelineObjectId(ExistsPrecondition.getClass)),
    preconditionFields = Precondition.defaultPreconditionFields
  )

}
