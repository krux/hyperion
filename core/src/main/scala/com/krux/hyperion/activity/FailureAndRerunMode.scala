package com.krux.hyperion.activity

trait FailureAndRerunMode {
  def toAws: String
}

object FailureAndRerunMode {

  case object CascadeOnFailure extends FailureAndRerunMode {

    def toAws = "cascade"

    override val toString: String = toAws

  }

  case object None extends FailureAndRerunMode {

    def toAws = "none"

    override val toString: String = toAws

  }
}
