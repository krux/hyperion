package com.krux.hyperion.resource

trait ActionOnResourceFailure

case object RetryAllOnResourceFailure {
  override def toString: String = "retryall"
}

case object RetryNoneOnResourceFailure {
  override def toString: String = "retrynone"
}
