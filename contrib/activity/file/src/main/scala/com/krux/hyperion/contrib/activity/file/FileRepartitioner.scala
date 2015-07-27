package com.krux.hyperion.contrib.activity.file

import java.io.File
import java.nio.file.{StandardCopyOption, Files, Paths}

case class FileRepartitioner(options: Options) {

  def repartition(): Boolean = moveFiles(split(merge()))

  private def merge(): File = options.inputs match {
    case Seq(one) => one

    case files =>
      val destination: File = File.createTempFile("merge-", ".tmp", options.temporaryDirectory.get)
      destination.deleteOnExit()
      FileMerger(destination, options.skipFirstLine).merge(options.inputs: _*)
  }

  private def split(file: File): Seq[File] = options.numberOfFiles match {
    case Some(1) => Seq(file)

    case None =>
      FileSplitter(
        header = options.header,
        numberOfLinesPerFile = options.numberOfLinesPerFile.getOrElse(Long.MaxValue),
        numberOfBytesPerFile = options.numberOfBytesPerFile.getOrElse(Long.MaxValue),
        bufferSize = options.bufferSize,
        compressed = options.compressed,
        temporaryDirectory = options.temporaryDirectory.get
      ).split(file)

    case Some(n) =>
      FileSplitter(
        header = options.header,
        numberOfLinesPerFile = Long.MaxValue,
        numberOfBytesPerFile = file.length() / n,
        bufferSize = options.bufferSize,
        compressed = options.compressed,
        temporaryDirectory = options.temporaryDirectory.get
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

  private def moveFiles(files: Seq[File]): Boolean = options.outputDirectory.forall { dir =>
    nameFiles(files).foreach { case (f, output) =>
      val source = Paths.get(f.getAbsolutePath)
      val dest = Paths.get(dir.getAbsolutePath, output)
      if (options.outputDirectory.size == 1) {
        Files.move(source, dest, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
      } else if (options.link) {
        Files.createSymbolicLink(dest, source)
      } else {
        Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING)
      }
    }

    true
  } match {
    case true if !options.link && options.outputDirectory.size > 1 => files.forall(f => f.delete())
    case x => x
  }

}
