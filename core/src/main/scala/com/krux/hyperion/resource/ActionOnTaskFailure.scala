package com.krux.hyperion.resource

trait ActionOnTaskFailure {
  def serialize: String
  override def toString = serialize
}

case object ContinueOnTaskFailure extends ActionOnTaskFailure {
  val serialize: String = "continue"
}

case object TerminateOnTaskFailure extends ActionOnTaskFailure {
  val serialize: String = "terminate"
}
