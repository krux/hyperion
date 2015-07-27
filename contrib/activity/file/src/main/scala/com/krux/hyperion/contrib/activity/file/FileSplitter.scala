package com.krux.hyperion.contrib.activity.file

import java.io._
import java.util.zip.GZIPOutputStream
import org.apache.commons.io.IOUtils

import scala.collection.mutable.ListBuffer

case class FileSplitter(
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

    def close(): Unit = outputStreamWriter.foreach(IOUtils.closeQuietly)

    def write(byte: Int): Unit = {
      numberOfBytes += 1
      outputStreamWriter.foreach(_.write(byte))
    }
  }

  private var fileState: FileState = new FileState

  def split(source: File): Seq[File] = try {
    val splits = ListBuffer[File]()
    val input = new BufferedInputStream(new FileInputStream(source), bufferSize)
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

    fileState = Option(new FileOutputStream(file, true))
      .map(s => if (compressed) new GZIPOutputStream(s) else s)
      .map(s => new BufferedOutputStream(s))
      .map(s => new FileState(Option(s)))
      .get

    header.map(_.getBytes).foreach(fileState.outputStreamWriter.get.write)

    file
  }

}
