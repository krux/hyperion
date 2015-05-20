package com.krux.hyperion.objects.precondition

import com.krux.hyperion.objects.PipelineObject
import com.krux.hyperion.objects.aws.{AdpPrecondition, AdpRef}

/**
 * The base trait of all preconditions.
 *
 * A precondition is a condition that must be met before the object can run. The activity cannot run
 * until all its conditions are met.
 */
trait Precondition extends PipelineObject {

  /**
   * The precondition will be retried until the retryTimeout with a gap of retryDelay between attempts.
   * Time period; for example, "1 hour".
   */
  def preconditionTimeout: Option[String]

  /**
   * The IAM role to use for this precondition.
   */
  def role: Option[String]

  def serialize: AdpPrecondition

  def ref: AdpRef[AdpPrecondition] = AdpRef(serialize)

}
