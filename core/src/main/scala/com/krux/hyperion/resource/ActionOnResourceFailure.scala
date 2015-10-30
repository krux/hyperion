package com.krux.hyperion.resource

trait ActionOnResourceFailure {
  def serialize: String
  override def toString = serialize
}

case object RetryAllOnResourceFailure extends ActionOnResourceFailure {
  val serialize: String = "retryall"
}

case object RetryNoneOnResourceFailure extends ActionOnResourceFailure {
  val serialize: String = "retrynone"
}
