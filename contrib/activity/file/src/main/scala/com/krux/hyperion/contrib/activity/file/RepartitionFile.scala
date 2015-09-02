package com.krux.hyperion.contrib.activity.file

import java.io.{FilenameFilter, File}
import java.nio.file._
import scopt.OptionParser

object RepartitionFile {

  def stringToOptionalFile(s: String): Option[File] = Option(s).map(Paths.get(_)).map(_.toFile)

  def applyDefaultTemporaryDirectory(options: Options): Options =
    options.copy(temporaryDirectory = options.temporaryDirectory
      .orElse(stringToOptionalFile(System.getenv("TMPDIR")))
      .orElse(stringToOptionalFile("/tmp"))
    )

  def applyDefaultNumberOfFilesCalculation(options: Options): Options =
    options.numberOfFiles.map { numberOfFiles =>
      if (numberOfFiles == 1) {
        options.copy(suffixLength = 0)
      } else {
        options
      }
    }.getOrElse(options)

  def applyDefaultFileChecks(options: Options): Options = options.copy(inputs = options.inputs.flatMap { k =>
    if (k.getName == "-") {
      Seq(k)
    } else if (k.isDirectory) {
      k.listFiles(new FilenameFilter {
        val matcher = options.pattern.map(pattern => FileSystems.getDefault.getPathMatcher(s"glob:$pattern"))

        override def accept(dir: File, name: String): Boolean =
          !name.startsWith("_") && matcher.forall(_.matches(Paths.get(name)))
      }).toSeq
    } else if (k.isFile) {
      if (k.canRead) {
        Seq(k.getAbsoluteFile)
      } else {
        System.err.println(s"ERROR: Cannot read $k")
        Seq()
      }
    } else {
      System.err.println(s"ERROR: Cannot access $k")
      Seq()
    }
  })

  def applyDefaultNumberOfFiles(options: Options): Options =
    if (options.numberOfFiles.isEmpty && options.numberOfLinesPerFile.isEmpty && options.numberOfBytesPerFile.isEmpty) {
      options.copy(numberOfFiles = Option(1))
    } else {
      options
    }

  def applyDefaultDirectory(options: Options): Options =
    if (options.outputDirectory.isEmpty) {
      if (System.getenv().containsKey("OUTPUT1_STAGING_DIR")) {
        options.copy(outputDirectory = options.outputDirectory ++ (1 until 11).flatMap(n => stringToOptionalFile(System.getenv(s"OUTPUT${n}_STAGING_DIR"))))
      } else {
        options.copy(outputDirectory = options.outputDirectory ++ stringToOptionalFile(System.getProperty("user.dir")))
      }
    } else {
      options
    }

  def applyDefaultInputs(options: Options): Options =
    if (options.inputs.isEmpty) {
      if (System.getenv().containsKey("INPUT1_STAGING_DIR")) {
        options.copy(inputs = options.inputs ++ (1 until 11).flatMap(n => stringToOptionalFile(System.getenv(s"INPUT${n}_STAGING_DIR"))))
      } else {
        options
      }
    } else {
      options
    }

  def applyDefaultCompression(options: Options): Options =
    if (options.compressed && !options.output.endsWith(".gz")) {
      options.copy(output = s"${options.output}.gz")
    } else {
      options
    }

  def applyDefaults(options: Options): Options =
    Seq(
      applyDefaultTemporaryDirectory _,
      applyDefaultInputs _,
      applyDefaultDirectory _,
      applyDefaultNumberOfFiles _,
      applyDefaultFileChecks _,
      applyDefaultNumberOfFilesCalculation _,
      applyDefaultCompression _
    ).foldLeft(options)((acc, handler) => handler(acc))

  def checkOptions(options: Options): Option[Options] = if (options.inputs.isEmpty) {
    System.err.println("ERROR: No inputs specified.")
    None
  } else if (options.outputDirectory.isEmpty) {
    System.err.println("ERROR: No outputs specified.")
    None
  } else {
    Option(options)
  }

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Options](s"hyperion-file-repartition-activity") {
      override def showUsageOnError = false

      note(
        """Repartitions a set of files into either a given number of files, lines per file or bytes per file.
          |Options including compressing the output and adding header to each file.
        """.stripMargin)

      help("help").text("prints this usage text")
      opt[Unit]('z', "compressed").optional().action((_, c) => c.copy(compressed = true))
        .text("gzip the output file (ext will have .gz added at the end")
      opt[Unit]("skip-first-line").optional().action((_, c) => c.copy(skipFirstLine = true))
        .text("assume the input files have a header in the first line and skip it")
      opt[Unit]('L', "link").optional().action((_, c) => c.copy(link = true))
        .text("link the output files instead of copying into position")
      opt[Unit]("mark-successful-jobs").optional().action((_, c) => c.copy(markSuccessfulJobs = true))
        .text("Creates a _SUCCESS file to mark the successful completion of the job")
      opt[String]('H', "header").valueName("HEAD").optional().action((x, c) => c.copy(header = Option(s"$x\n")))
        .text("prepend header HEAD to each file")
      opt[Int]('a', "suffix-length").valueName("N").optional().action((x, c) => c.copy(suffixLength = x))
        .text("use suffixes of length N (default: 5)").validate(x => if (1 <= x && x <= 10) success else failure("Suffix length must be between 1 and 10"))
      opt[Int]('n', "files").valueName("N").optional().action((x, c) => c.copy(numberOfFiles = Option(x)))
        .text("create N of files of roughly equal size").validate(x => if (x > 0) success else failure("Files must be positive"))
      opt[Long]('l', "lines").valueName("N").optional().action((x, c) => c.copy(numberOfLinesPerFile = Option(x)))
        .text("create smaller files than N number of lines").validate(x => if (x > 0) success else failure("Lines must be positive"))
      opt[String]('C', "line-bytes").valueName("N").optional().action((x, c) => c.copy(numberOfBytesPerFile = Option(StorageUnit.parse(x))))
        .text("create smaller files than N number of bytes")
      opt[String]('S', "buffer-size").valueName("N").optional().action((x, c) => c.copy(bufferSize = StorageUnit.parse(x)))
        .text("use N bytes for main memory buffer (default: 8192)")
      opt[File]('i', "input").valueName("PATH").optional().unbounded().action((x, c) => c.copy(inputs = c.inputs :+ x))
        .text("Use PATH as input.  If PATH is a directory, then all files within the directory are used as inputs.")
      opt[File]('o', "output").valueName("DIR").optional().unbounded().action((x, c) => c.copy(outputDirectory = c.outputDirectory :+ x))
        .text("create the output files in DIR").validate(x => if (x.exists) success else failure("Directory must exist"))
      opt[File]('T', "temporary-directory").valueName("DIR").optional().action((x, c) => c.copy(temporaryDirectory = Option(x)))
        .text(s"use DIR for temporaries, not $$TMPDIR or /tmp").validate(x => if (x.exists) success else failure("Directory must exist"))
      opt[String]("name").valueName("PATTERN").optional().action((x, c) => c.copy(pattern = Option(x)))
        .text("Base of input file names (the path with the leading directories removed) matches shell pattern PATTERN.")
      arg[String]("NAME").required().action((x, c) => c.copy(output = x))
        .text("use NAME for the output filename.  The actual files will have suffixes of suffix-length")

      note(s"\nIf --input PATH is not specified, then directories specified by $${INPUT1_STAGING_DIR}..$${INPUT10_STAGING_DIR} are searched for files.\n")
      note(s"If --output PATH is not specified, then directories specified by $${OUTPUT1_STAGING_DIR}..$${OUTPUT10_STAGING_DIR} are used.")
      note("If those directories are not specified, then the current directory is used.")

      checkConfig { c =>
        if (c.numberOfLinesPerFile.nonEmpty && c.numberOfBytesPerFile.nonEmpty) {
          failure("cannot specify both number of lines and number of bytes")
        } else if (c.numberOfFiles.nonEmpty && (c.numberOfLinesPerFile.nonEmpty || c.numberOfBytesPerFile.nonEmpty)) {
          failure("cannot specify both number of files and number of lines/bytes")
        } else {
          success
        }
      }
    }

    if (!parser.parse(args, Options()).map(applyDefaults).flatMap(checkOptions).exists(FileRepartitioner(_).repartition())) {
      parser.showUsageAsError
      System.exit(3)
    }
  }

}

