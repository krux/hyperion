package com.krux.hyperion.expressions

/**
 * AWS DataPipeline available runtime field that references date time fields
 */
object DateTimeRef extends Enumeration {
  type DateTimeRef = Value
  // The date and time that the scheduled run actually started. This is a runtime slot.
  val ActualStartTime = Value("@actualStartTime")
  // The date and time that the scheduled run actually ended. This is a runtime slot.
  val ActualEndTime = Value("@actualEndTime")
  // The date and time that the run was scheduled to start.
  val ScheduledStartTime = Value("@scheduledStartTime")
  // The date and time that the run was scheduled to end.
  val ScheduledEndTime = Value("@scheduledEndTime")
  // The last time that Task Runner, or other code that is processing the tasks, called the
  // ReportTaskProgress API.
  val reportProgressTime = Value("@reportProgressTime")
}
