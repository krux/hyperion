package com.krux.hyperion.objects

/**
 * A condition that must be met before the object can run.
 * The activity cannot run until all its conditions are met.
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
}