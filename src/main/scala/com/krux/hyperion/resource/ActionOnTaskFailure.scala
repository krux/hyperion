package com.krux.hyperion.resource

trait ActionOnTaskFailure

case object ContinueOnTaskFailure {
  override def toString: String = "continue"
}

case object TerminateOnTaskFailure {
  override def toString: String = "terminate"
}
