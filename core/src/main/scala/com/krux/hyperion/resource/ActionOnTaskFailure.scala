package com.krux.hyperion.resource

trait ActionOnTaskFailure {
  def serialize: String
}

case object ContinueOnTaskFailure extends ActionOnTaskFailure {
  val serialize: String = "continue"
  override val toString = serialize
}

case object TerminateOnTaskFailure extends ActionOnTaskFailure {
  val serialize: String = "terminate"
  override val toString = serialize
}
