package com.krux.hyperion.precondition

import com.krux.hyperion.adt.{ HDuration, HString }
import com.krux.hyperion.aws.{ AdpRef, AdpPrecondition }
import com.krux.hyperion.common.{ NamedPipelineObject, PipelineObject }
import com.krux.hyperion.HyperionContext

/**
 * The base trait of all preconditions.
 *
 * A precondition is a condition that must be met before the object can run. The activity cannot run
 * until all its conditions are met.
 */
trait Precondition extends NamedPipelineObject {

  type Self <: Precondition

  def preconditionFields: PreconditionFields
  def updatePreconditionFields(fields: PreconditionFields): Self

  /**
   * The IAM role to use for this precondition.
   */
  def role: HString = preconditionFields.role
  def withRole(role: HString) = updatePreconditionFields(
    preconditionFields.copy(role = role)
  )

  /**
   * The precondition will be retried until the retryTimeout with a gap of retryDelay between attempts.
   * Time period; for example, "1 hour".
   */
  def preconditionTimeout: Option[HDuration] = preconditionFields.preconditionTimeout
  def withPreconditionTimeout(timeout: HDuration) = updatePreconditionFields(
    preconditionFields.copy(preconditionTimeout = Option(timeout))
  )

  def serialize: AdpPrecondition

  def ref: AdpRef[AdpPrecondition] = AdpRef(serialize)

  def objects: Iterable[PipelineObject] = None

}

object Precondition {

  def defaultPreconditionFields(implicit hc: HyperionContext) = PreconditionFields(
    role = hc.role,
    preconditionTimeout = None
  )

}
