package com.krux.hyperion.contrib.activity.file

import java.io._
import java.util.zip.{GZIPOutputStream, GZIPInputStream}

import org.apache.commons.io.IOUtils

case class FileMerger(destination: File, skipFirstLine: Boolean = false, headers: Option[String] = None) {
  def merge(sources: File*): File = {
    val output: OutputStream = new BufferedOutputStream({
      val s = new FileOutputStream(destination, true)
      if (destination.getName.endsWith(".gz")) new GZIPOutputStream(s) else s
    })

    try {
      sources.foldLeft(headers -> output)(appendFile)
      destination
    } finally {
      IOUtils.closeQuietly(output)
    }
  }

  private def doSkipFirstLine(input: InputStream): InputStream = {
    while (skipFirstLine && (input.read() match {
      case -1 | '\n' => false
      case _ => true
    })) {}

    input
  }

  private def appendFile(state: (Option[String], OutputStream), source: File): (Option[String], OutputStream) = {
    val (headers, output) = state

    if (source.getName == "-") {
      print("Merging stdin...")
      headers.map(_.getBytes).foreach(output.write)
      IOUtils.copy(doSkipFirstLine(System.in), output)
      println("done")

      None -> output
    } else if (source.length() > 0) {
      print(s"Merging ${source.getAbsolutePath}...")

      val input = new BufferedInputStream({
        val s = new FileInputStream(source)
        if (source.getName.endsWith(".gz")) new GZIPInputStream(s) else s
      })

      // If the input doesn't have at least 2 bytes available then it is most likely empty
      try {
        if (input.available() > 1) {
          headers.map(_.getBytes).foreach(output.write)

          IOUtils.copy(doSkipFirstLine(input), output)
        }
      } finally {
        IOUtils.closeQuietly(input)
        println("done")
      }

      None -> output
    } else {
      headers -> output
    }
  }
}
