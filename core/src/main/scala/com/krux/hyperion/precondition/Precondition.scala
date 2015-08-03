package com.krux.hyperion.precondition

import com.krux.hyperion.aws.{AdpRef, AdpPrecondition}
import com.krux.hyperion.common.PipelineObject
import com.krux.hyperion.expression.Duration
import com.krux.hyperion.parameter.Parameter

/**
 * The base trait of all preconditions.
 *
 * A precondition is a condition that must be met before the object can run. The activity cannot run
 * until all its conditions are met.
 */
trait Precondition extends PipelineObject {

  /**
   * The IAM role to use for this precondition.
   */
  def role: String

  /**
   * The precondition will be retried until the retryTimeout with a gap of retryDelay between attempts.
   * Time period; for example, "1 hour".
   */
  def preconditionTimeout: Option[Parameter[Duration]]

  def serialize: AdpPrecondition

  def ref: AdpRef[AdpPrecondition] = AdpRef(serialize)

  def objects: Iterable[PipelineObject] = None

}
