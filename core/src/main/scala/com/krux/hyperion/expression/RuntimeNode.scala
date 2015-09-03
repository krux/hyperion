package com.krux.hyperion.expression

class RuntimeNode(runtimeSlot: RuntimeSlot) extends Expression {

  def content: String =  "node." + runtimeSlot.ref

}

object RuntimeNode {

  // The date and time that the scheduled run actually ended.
  val actualEndTime = new RuntimeNode(RuntimeSlot.actualEndTime)

  // The date and time that the scheduled run actually started.
  val actualStartTime = new RuntimeNode(RuntimeSlot.actualStartTime)

  // The date and time that the run was scheduled to end.
  val scheduledEndTime = new RuntimeNode(RuntimeSlot.scheduledEndTime)

  // The date and time that the run was scheduled to start.
  val scheduledStartTime = new RuntimeNode(RuntimeSlot.scheduledStartTime)

  // The last time that Task Runner, or other code that is processing the tasks, called the
  // ReportTaskProgress operation.
  val reportProgressTime = new RuntimeNode(RuntimeSlot.reportProgressTime)

  // The host name of client that picked up the task attempt.
  val hostname = new RuntimeNode(RuntimeSlot.hostname)

  // The status of this object. Possible values are: pending, waiting_on_dependencies, running,
  // waiting_on_runner, successful, and failed.
  val status = new RuntimeNode(RuntimeSlot.status)

  // A list of all objects that this object is waiting on before it can enter the RUNNING state.
  val waitingOn = new RuntimeNode(RuntimeSlot.waitingOn)

  // The number of attempted runs remaining before setting the status of this object to failed.
  val triesLeft = new RuntimeNode(RuntimeSlot.triesLeft)

  // The health status of the object, which reflects success or failure of the last instance that
  // reached a terminated state. Values are: HEALTHY or ERROR.
  val healthStatus = new RuntimeNode(RuntimeSlot.healthStatus)

  // The ID of the last object instance that reached a terminated state.
  val healthStatusFromInstanceId = new RuntimeNode(RuntimeSlot.healthStatusFromInstanceId)

  // The last time at which the health status was updated.
  val healthStatusUpdatedTime = new RuntimeNode(RuntimeSlot.healthStatusUpdatedTime)

  // If the object failed, the error code.
  val errorId = new RuntimeNode(RuntimeSlot.errorId)

  // If the object failed, the error message.
  val errorMessage = new RuntimeNode(RuntimeSlot.errorMessage)

  // If the object failed, the error stack trace.
  val errorStackTrace = new RuntimeNode(RuntimeSlot.errorStackTrace)

  // The status most recently reported from the object.
  val attemptStatus = new RuntimeNode(RuntimeSlot.attemptStatus)

  // The reason for the failure to create the resource.
  val failureReason = new RuntimeNode(RuntimeSlot.failureReason)

}
