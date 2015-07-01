package com.krux.hyperion.common

trait StorageClass

object StorageClass {
  object Standard extends StorageClass {
    override def toString: String = "STANDARD"
  }

  object ReducedRedundancy extends StorageClass {
    override def toString: String = "REDUCED_REDUNDANCY"
  }
}
