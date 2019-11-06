package com.krux.hyperion.enum

object CompressionFormat extends Enumeration {
  type CompressionFormat = Value

  val GZ = Value("gz")
  val BZ2 = Value("bz2")
}
