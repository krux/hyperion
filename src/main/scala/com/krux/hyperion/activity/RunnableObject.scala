package com.krux.hyperion.activity

import com.krux.hyperion.expression.DateTimeRef._

/**
 * Run time references of runnable objects
 */
trait RunnableObject {

  def actualStartTime: DateTimeRef = ActualStartTime

  def actualEndTime: DateTimeRef = ActualEndTime

  def scheduledStartTime: DateTimeRef = ScheduledStartTime

  def scheduledEndTime: DateTimeRef = ScheduledEndTime

}
