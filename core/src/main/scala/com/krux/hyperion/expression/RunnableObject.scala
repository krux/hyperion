package com.krux.hyperion.expression

/**
 * All fields that the object 
 */
trait RunnableObject { self =>

  def objectName: String = ""

  /**
   * The date and time that the scheduled run actually ended.
   */
  case object ActualEndTime extends RefExpression with DateTimeTypedExp {
    override val objectName = self.objectName
    val isRuntime = true
    val refName = "actualEndTime"
  }

  /**
   * The date and time that the scheduled run actually started.
   */
  case object ActualStartTime extends RefExpression with DateTimeTypedExp {
    override val objectName = self.objectName
    val isRuntime = true
    val refName = "actualStartTime"
  }

  /**
   * The date and time that the run was scheduled to end.
   */
  case object ScheduledEndTime extends RefExpression with DateTimeTypedExp {
    override val objectName = self.objectName
    val isRuntime = true
    val refName = "scheduledEndTime"
  }

  /**
   * The date and time that the run was scheduled to start.
   */
  case object ScheduledStartTime extends RefExpression with DateTimeTypedExp {
    override val objectName = self.objectName
    val isRuntime = true
    val refName = "scheduledStartTime"
  }

  /**
   * The last time that Task Runner, or other code that is processing the tasks, called the ReportTaskProgress operation.
   */
  case object ReportProgressTime extends RefExpression with DateTimeTypedExp {
    override val objectName = self.objectName
    val isRuntime = true
    val refName = "reportProgressTime"
  }

  /**
   * The host name of client that picked up the task attempt.
   */
  case object Hostname extends RefExpression with DateTimeTypedExp {
    override val objectName = self.objectName
    val isRuntime = true
    val refName = "hostname"
  }

  /**
   * The status of this object. Possible values are: pending, waiting_on_dependencies, running, waiting_on_runner, successful, and failed.
   */
  case object Status extends RefExpression with StringTypedExp {
    override val objectName = self.objectName
    val isRuntime = true
    val refName = "status"
  }

  /**
   * A list of all objects that this object is waiting on before it can enter the RUNNING state.
   */
  case object WaitingOn extends RefExpression with StringTypedExp {
    override val objectName = self.objectName
    val isRuntime = true
    val refName = "waitingOn"
  }

  /**
   * The number of attempted runs remaining before setting the status of this object to failed.
   */
  case object TriesLeft extends RefExpression with IntTypedExp {
    override val objectName = self.objectName
    val isRuntime = true
    val refName = "triesLeft"
  }

  /**
   * The reason for the failure to create the resource.
   */
  case object FailureReason extends RefExpression with StringTypedExp {
    override val objectName = self.objectName
    val isRuntime = true
    val refName = "failureReason"
  }

}

object RunnableObject extends RunnableObject
