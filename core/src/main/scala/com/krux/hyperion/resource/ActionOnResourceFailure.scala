package com.krux.hyperion.resource

trait ActionOnResourceFailure

case object RetryAllOnResourceFailure extends ActionOnResourceFailure {
  override def toString: String = "retryall"
}

case object RetryNoneOnResourceFailure extends ActionOnResourceFailure {
  override def toString: String = "retrynone"
}
