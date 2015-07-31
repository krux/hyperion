package com.krux.hyperion.activity

trait FailureAndRerunMode

object FailureAndRerunMode {
  case object CascadeOnFailure extends FailureAndRerunMode {
    override val toString: String = "cascade"
  }

  case object None extends FailureAndRerunMode {
    override val toString: String = "none"
  }
}
