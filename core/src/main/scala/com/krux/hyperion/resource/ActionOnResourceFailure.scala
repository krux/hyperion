package com.krux.hyperion.resource

trait ActionOnResourceFailure {
  def serialize: String
}

case object RetryAllOnResourceFailure extends ActionOnResourceFailure {
  val serialize: String = "retryall"
  override val toString = serialize
}

case object RetryNoneOnResourceFailure extends ActionOnResourceFailure {
  val serialize: String = "retrynone"
  override val toString = serialize
}
