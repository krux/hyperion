package com.krux.hyperion.activity

import com.krux.hyperion.expression.RuntimeSlot

/**
 * Run time references of runnable objects
 */
trait RunnableObject {

  // The date and time that the scheduled run actually started. This is a runtime slot.
  val actualStartTime = RuntimeSlot.actualStartTime

  // The date and time that the scheduled run actually ended. This is a runtime slot.
  val actualEndTime = RuntimeSlot.actualEndTime

  // The date and time that the run was scheduled to start.
  val scheduledStartTime = RuntimeSlot.scheduledStartTime

  // The date and time that the run was scheduled to end.
  val scheduledEndTime = RuntimeSlot.scheduledEndTime

}
