package com.krux.hyperion.expression

/**
 * All fields that the object
 */
trait RunnableObject { self =>

  def objectName: Option[String] = None

  /**
   * The date and time that the scheduled run actually ended.
   */
  case object ActualEndTime extends ReferenceExpression with DateTimeExp {
    override val objectName = self.objectName
    val isRuntime = true
    val referenceName = "actualEndTime"
  }

  /**
   * The date and time that the scheduled run actually started.
   */
  case object ActualStartTime extends ReferenceExpression with DateTimeExp {
    override val objectName = self.objectName
    val isRuntime = true
    val referenceName = "actualStartTime"
  }

  /**
   * The date and time that the run was scheduled to end.
   */
  case object ScheduledEndTime extends ReferenceExpression with DateTimeExp {
    override val objectName = self.objectName
    val isRuntime = true
    val referenceName = "scheduledEndTime"
  }

  /**
   * The date and time that the run was scheduled to start.
   */
  case object ScheduledStartTime extends ReferenceExpression with DateTimeExp {
    override val objectName = self.objectName
    val isRuntime = true
    val referenceName = "scheduledStartTime"
  }

  /**
   * The last time that Task Runner, or other code that is processing the tasks, called the ReportTaskProgress operation.
   */
  case object ReportProgressTime extends ReferenceExpression with DateTimeExp {
    override val objectName = self.objectName
    val isRuntime = true
    val referenceName = "reportProgressTime"
  }

  /**
   * The host name of client that picked up the task attempt.
   */
  case object Hostname extends ReferenceExpression with DateTimeExp {
    override val objectName = self.objectName
    val isRuntime = true
    val referenceName = "hostname"
  }

  /**
   * The status of this object. Possible values are: pending, waiting_on_dependencies, running, waiting_on_runner, successful, and failed.
   */
  case object Status extends ReferenceExpression with StringExp {
    override val objectName = self.objectName
    val isRuntime = true
    val referenceName = "status"
  }

  /**
   * A list of all objects that this object is waiting on before it can enter the RUNNING state.
   */
  case object WaitingOn extends ReferenceExpression with StringExp {
    override val objectName = self.objectName
    val isRuntime = true
    val referenceName = "waitingOn"
  }

  /**
   * The number of attempted runs remaining before setting the status of this object to failed.
   */
  case object TriesLeft extends ReferenceExpression with IntExp {
    override val objectName = self.objectName
    val isRuntime = true
    val referenceName = "triesLeft"
  }

  /**
   * The reason for the failure to create the resource.
   */
  case object FailureReason extends ReferenceExpression with StringExp {
    override val objectName = self.objectName
    val isRuntime = true
    val referenceName = "failureReason"
  }

}

object RunnableObject extends RunnableObject
