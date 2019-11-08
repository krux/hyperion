package com.krux.hyperion.contrib.activity.file

import java.io.File
import java.nio.file.{AtomicMoveNotSupportedException, Files, Paths, StandardCopyOption}

import com.krux.hyperion.contrib.activity.file.enum.CompressionFormat

case class FileRepartitioner(options: Options) {

  def repartition(): Boolean = moveFiles(nameFiles(split(merge())))

  private def merge(): File = options.inputs match {
    case Seq(one) if options.numberOfFiles != Some(1) || options.header.isEmpty => one

    case files =>
      val compressionExtension =
        if (options.compressed && options.compressionFormat == CompressionFormat.GZ) ".gz"
        else if (options.compressed && options.compressionFormat == CompressionFormat.BZ2) ".bz2"
        else ".tmp"
      val destination: File = File.createTempFile("merge-", compressionExtension, options.temporaryDirectory.get)
      destination.deleteOnExit()

      // If we are simply merging files then the merge step needs to add the header.
      val headers = options.numberOfFiles match {
        case Some(1) => options.header
        case _ => None
      }

      FileMerger(destination, options.skipFirstLine, headers).merge(options.inputs: _*)
  }

  private def split(file: File): Seq[File] = options.numberOfFiles match {
    case Some(1) => Seq(file)

    case None =>
      new FileSplitter(
        header = options.header,
        numberOfLinesPerFile = options.numberOfLinesPerFile.getOrElse(Long.MaxValue),
        numberOfBytesPerFile = options.numberOfBytesPerFile.getOrElse(Long.MaxValue),
        bufferSize = options.bufferSize,
        compressed = options.compressed,
        temporaryDirectory = options.temporaryDirectory.get,
        compressionFormat = options.compressionFormat
      ).split(file)

    case Some(n) =>
      new FileSplitter(
        header = options.header,
        numberOfLinesPerFile = Long.MaxValue,
        numberOfBytesPerFile = file.length() / n,
        bufferSize = options.bufferSize,
        compressed = options.compressed,
        temporaryDirectory = options.temporaryDirectory.get,
        compressionFormat = options.compressionFormat
      ).split(file)
  }

  private def nameFiles(files: Seq[File]): Map[File, String] = files match {
    case Seq(f) =>
      Map(f -> options.output)

    case mergedFiles =>
      val fmt = s"%0${options.suffixLength}d"

      mergedFiles.zipWithIndex.flatMap { case (f, i) =>
        options.output.split('.').toList match {
          case h :: Nil => Option(f -> s"$h-${fmt.format(i)}")
          case h :: t => Option(f -> s"$h-${fmt.format(i)}.${t.mkString(".")}")
          case Nil => None
        }
      }.toMap
  }

  private def moveFiles(files: Map[File, String]): Boolean = options.outputDirectory.forall { dir =>
    files.foreach { case (f, output) =>
      val source = Paths.get(f.getAbsolutePath)
      val dest = Paths.get(dir.getAbsolutePath, output)
      if (options.outputDirectory.size == 1) {
        try {
          // First try to atomically move
          Files.move(source, dest, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        } catch {
          case e: AtomicMoveNotSupportedException =>
            // Try to non-atomically move
            Files.move(source, dest, StandardCopyOption.REPLACE_EXISTING)
        }
      } else if (options.link) {
        Files.createSymbolicLink(dest, source)
      } else {
        Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING)
      }
    }

    // Mark a successful job
    if (options.markSuccessfulJobs) {
      Paths.get(dir.getPath, "_SUCCESS").toFile.createNewFile()
    }

    true
  } match {
    case true if !options.link && options.outputDirectory.size > 1 => files.keys.forall(f => f.delete())
    case x => x
  }

}
