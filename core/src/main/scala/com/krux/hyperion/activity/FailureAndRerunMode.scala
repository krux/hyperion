package com.krux.hyperion.activity

trait FailureAndRerunMode {
  def serialize: String
}

object FailureAndRerunMode {

  case object CascadeOnFailure extends FailureAndRerunMode {

    def serialize = "cascade"

    override val toString: String = serialize

  }

  case object None extends FailureAndRerunMode {

    def serialize = "none"

    override val toString: String = serialize

  }
}
