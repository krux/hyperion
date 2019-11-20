package com.krux.hyperion.contrib.activity.file

object CompressionFormat extends Enumeration {
  type CompressionFormat = Value

  val GZ = Value("gz")
  val BZ2 = Value("bz2")
}
