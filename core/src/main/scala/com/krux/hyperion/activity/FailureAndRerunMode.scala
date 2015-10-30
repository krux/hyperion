package com.krux.hyperion.activity

trait FailureAndRerunMode {

  def serialize: String

  override def toString: String = serialize

}

object FailureAndRerunMode {

  case object CascadeOnFailure extends FailureAndRerunMode {
    val serialize = "cascade"
  }

  case object None extends FailureAndRerunMode {
    val serialize = "none"
  }
}
