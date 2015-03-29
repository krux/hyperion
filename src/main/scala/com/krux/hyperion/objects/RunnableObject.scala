package com.krux.hyperion.objects

import com.krux.hyperion.expressions.DateTimeRef._

/**
 * Run time references of runnable objects
 */
trait RunnableObject {

  def actualStartTime: DateTimeRef = ActualStartTime
  def actualEndTime: DateTimeRef = ActualEndTime
  def scheduledStartTime: DateTimeRef = ScheduledStartTime
  def scheduledEndTime: DateTimeRef = ScheduledEndTime

}
