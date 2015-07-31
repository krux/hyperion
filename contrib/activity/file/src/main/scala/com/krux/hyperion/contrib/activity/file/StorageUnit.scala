package com.krux.hyperion.contrib.activity.file

object StorageUnit {
  val units = Seq(
    "KB" -> 1000L,
    "MB" -> 1000L * 1000L,
    "GB" -> 1000L * 1000L * 1000L,
    "TB" -> 1000L * 1000L * 1000L * 1000L,
    "PB" -> 1000L * 1000L * 1000L * 1000L * 1000L,
    "EB" -> 1000L * 1000L * 1000L * 1000L * 1000L * 1000L,
    "ZB" -> 1000L * 1000L * 1000L * 1000L * 1000L * 1000L * 1000L,
    "YB" -> 1000L * 1000L * 1000L * 1000L * 1000L * 1000L * 1000L * 1000L,
    "KIB" -> 1024L,
    "MIB" -> 1024L * 1024L,
    "GIB" -> 1024L * 1024L * 1024L,
    "TIB" -> 1024L * 1024L * 1024L * 1024L,
    "PIB" -> 1024L * 1024L * 1024L * 1024L * 1024L,
    "EIB" -> 1024L * 1024L * 1024L * 1024L * 1024L * 1024L,
    "ZIB" -> 1024L * 1024L * 1024L * 1024L * 1024L * 1024L * 1024L,
    "YIB" -> 1024L * 1024L * 1024L * 1024L * 1024L * 1024L * 1024L * 1024L,
    "B" -> 1L
  )

  def parse(s: String): Long = {
    val trimmed = s.trim.toUpperCase
    val unit = units.toSeq.find(kv => trimmed.endsWith(kv._1)).getOrElse("B" -> 1L)
    trimmed.stripSuffix(unit._1).trim.toLong * unit._2
  }
}
