package com.krux.hyperion.contrib.activity.file

import java.io._

import org.apache.commons.io.IOUtils

case class FileMerger(destination: File, skipFirstLine: Boolean = false) {
  def merge(sources: File*): File = {
    val output: OutputStream = new BufferedOutputStream(new FileOutputStream(destination, true))
    try {
      sources.foldLeft(output)(appendFile)
      destination
    } finally {
      IOUtils.closeQuietly(output)
    }
  }

  private def doSkipFirstLine(input: InputStream): InputStream = {
    while (skipFirstLine && (input.read() match {
      case -1 => false
      case '\n' => false
      case _ => true
    })) {}

    input
  }

  private def appendFile(output: OutputStream, source: File): OutputStream = {
    if (source.getName == "-") {
      IOUtils.copy(doSkipFirstLine(System.in), output)
    } else {
      val input = new BufferedInputStream(new FileInputStream(source))
      try {
        IOUtils.copy(doSkipFirstLine(input), output)
      } finally {
        IOUtils.closeQuietly(input)
      }
    }
    output
  }
}
