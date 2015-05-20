package com.krux.hyperion.objects.action

import com.krux.hyperion.objects.{TerminateObjectId, PipelineObject}
import com.krux.hyperion.objects.aws.{AdpRef, AdpTerminate}

/**
 * An action to trigger the cancellation of a pending or unfinished activity, resource, or data
 * node. AWS Data Pipeline attempts to put the activity, resource, or data node into the CANCELLED
 * state if it does not finish by the lateAfterTimeout value.
 */
object Terminate extends PipelineObject {

  val id = TerminateObjectId

  def serialize = new AdpTerminate()
  def ref: AdpRef[AdpTerminate] = AdpRef(serialize)

}
