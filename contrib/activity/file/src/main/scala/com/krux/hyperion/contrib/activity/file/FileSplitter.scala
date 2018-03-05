package com.krux.hyperion.contrib.activity.file

import java.io._
import java.util.zip.{ GZIPInputStream, GZIPOutputStream }

import scala.collection.mutable.ListBuffer


class FileSplitter(
  header: Option[String],
  numberOfLinesPerFile: Long = Long.MaxValue,
  numberOfBytesPerFile: Long = Long.MaxValue,
  bufferSize: Long,
  compressed: Boolean,
  temporaryDirectory: File
) {
  private class FileState(
    val outputStreamWriter: Option[OutputStream] = None
  ) {
    var numberOfLines: Long = 0L
    var numberOfBytes: Long = 0L

    def isEmpty: Boolean = outputStreamWriter.isEmpty

    def close(): Unit = outputStreamWriter.foreach { o =>
      try {
        o.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }

    def write(byte: Int): Unit = {
      numberOfBytes += 1
      outputStreamWriter.foreach(_.write(byte))
    }
  }

  private var fileState: FileState = new FileState

  def split(source: File): Seq[File] = try {
    val splits = ListBuffer[File]()
    val input = new BufferedInputStream({
      val s = new FileInputStream(source)
      if (source.getName.endsWith(".gz")) new GZIPInputStream(s) else s
    }, bufferSize.toInt)
    var needFile = true

    var read = input.read()
    while (read != -1) {
      if (needFile) {
        val split = startNewFile()
        splits += split

        println(s"Creating split #${splits.size}: ${split.getAbsolutePath}")
        needFile = false
      }

      fileState.write(read)

      if (read == '\n') {
        fileState.numberOfLines += 1
        needFile = (fileState.numberOfLines >= numberOfLinesPerFile) || (fileState.numberOfBytes >= numberOfBytesPerFile)
      }

      read = input.read()
    }

    splits.toSeq
  } finally {
    fileState.close()
    fileState = new FileState
  }

  private def startNewFile(): File = {
    fileState.close()

    val file = File.createTempFile("split-", ".tmp", temporaryDirectory)

    fileState = new FileState(Option(new BufferedOutputStream({
      val s = new FileOutputStream(file, true)
      if (compressed) new GZIPOutputStream(s) else s
    })))

    header.map(_.getBytes).foreach { b =>
      fileState.numberOfBytes += b.length
      fileState.outputStreamWriter.get.write(b)
      fileState.numberOfLines += 1
    }

    file
  }

}
