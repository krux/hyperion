package com.krux.hyperion.common

trait StorageClass

object StorageClass {
  object Standard extends StorageClass {
    override def toString = "STANDARD"
  }

  object ReducedRedundancy extends StorageClass {
    override def toString = "REDUCED_REDUNDANCY"
  }
}
