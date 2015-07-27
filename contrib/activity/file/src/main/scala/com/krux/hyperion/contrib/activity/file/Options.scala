package com.krux.hyperion.contrib.activity.file

import java.io.File

case class Options(
  header: Option[String] = None,
  pattern: Option[String] = None,
  output: String = "",
  numberOfFiles: Option[Int] = None,
  numberOfLinesPerFile: Option[Long] = None,
  numberOfBytesPerFile: Option[Long] = None,
  suffixLength: Int = 5,
  bufferSize: Long = 8192,
  compressed: Boolean = false,
  link: Boolean = false,
  skipFirstLine: Boolean = false,
  inputs: Seq[File] = Seq(),
  outputDirectory: Seq[File] = Seq(),
  temporaryDirectory: Option[File] = None
)
