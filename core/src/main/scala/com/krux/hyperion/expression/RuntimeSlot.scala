package com.krux.hyperion.expression

sealed trait RuntimeSlot {
  def ref: String
  override def toString: String = ref
}

case class StringRuntimeSlot(ref: String) extends RuntimeSlot

case class IntegerRuntimeSlot(ref: String) extends RuntimeSlot

case class DateTimeRuntimeSlot(ref: String) extends RuntimeSlot

object RuntimeSlot {

  // The date and time that the scheduled run actually ended.
  val actualEndTime = DateTimeRuntimeSlot("@actualEndTime")

  // The date and time that the scheduled run actually started.
  val actualStartTime = DateTimeRuntimeSlot("@actualStartTime")

  // The date and time that the run was scheduled to end.
  val scheduledEndTime = DateTimeRuntimeSlot("@scheduledEndTime")

  // The date and time that the run was scheduled to start.
  val scheduledStartTime = DateTimeRuntimeSlot("@scheduledStartTime")

  // The last time that Task Runner, or other code that is processing the tasks, called the
  // ReportTaskProgress operation.
  val reportProgressTime = DateTimeRuntimeSlot("@reportProgressTime")

  // The host name of client that picked up the task attempt.
  val hostname = StringRuntimeSlot("@hostname")

  // The status of this object. Possible values are: pending, waiting_on_dependencies, running,
  // waiting_on_runner, successful, and failed.
  val status = StringRuntimeSlot("@status")

  // A list of all objects that this object is waiting on before it can enter the RUNNING state.
  val waitingOn = StringRuntimeSlot("@waitingOn")

  // The number of attempted runs remaining before setting the status of this object to failed.
  val triesLeft = IntegerRuntimeSlot("@triesLeft")

  // The health status of the object, which reflects success or failure of the last instance that
  // reached a terminated state. Values are: HEALTHY or ERROR.
  val healthStatus = StringRuntimeSlot("healthStatus")

  // The ID of the last object instance that reached a terminated state.
  val healthStatusFromInstanceId = StringRuntimeSlot("healthStatusFromInstanceId")

  // The last time at which the health status was updated.
  val healthStatusUpdatedTime = DateTimeRuntimeSlot("healthStatusUpdatedTime")

  // If the object failed, the error code.
  val errorId = StringRuntimeSlot("errorId")

  // If the object failed, the error message.
  val errorMessage = StringRuntimeSlot("errorMessage")

  // If the object failed, the error stack trace.
  val errorStackTrace = StringRuntimeSlot("errorStackTrace")

  // The status most recently reported from the object.
  val attemptStatus	 = StringRuntimeSlot("attemptStatus")

  // The reason for the failure to create the resource.
  val failureReason = StringRuntimeSlot("@failureReason")

}
