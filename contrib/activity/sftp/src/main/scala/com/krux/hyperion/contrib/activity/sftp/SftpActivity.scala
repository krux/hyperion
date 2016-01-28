package com.krux.hyperion.contrib.activity.sftp

import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }

import java.io._
import java.nio.file.Paths

import com.amazonaws.services.s3.AmazonS3Client
import com.jcraft.jsch.{ ChannelSftp, JSch, JSchException, UserInfo }
import org.joda.time.DateTime
import scopt.OptionParser

object SftpActivity {
  sealed trait Action

  case object UploadAction extends Action {
    override def toString = "upload"
  }

  case object DownloadAction extends Action {
    override def toString = "download"
  }

  case class Options(
    mode: Option[Action] = None,
    host: String = "",
    port: Option[Int] = None,
    username: Option[String] = None,
    password: Option[String] = None,
    identity: Option[String] = None,
    path: Option[String] = None,
    pattern: Option[String] = None,
    since: Option[DateTime] = None,
    until: Option[DateTime] = None,
    skipEmpty: Boolean = false,
    markSuccessfulJobs: Boolean = false
  )

  private def fileToByteArray(file: File): Array[Byte] = {
    val stream = new FileInputStream(file)
    try {
      streamToByteArray(stream)
    } finally {
      stream.close()
    }
  }

  private def streamToByteArray(stream: InputStream): Array[Byte] = {
    val buffer = new Array[Byte](1024)
    val os = new ByteArrayOutputStream()
    var line: Int = 0

    line = stream.read(buffer)
    while (line != -1) {
      os.write(buffer, 0, line)
      line = stream.read(buffer)
    }

    stream.close()
    os.flush()
    os.close()
    os.toByteArray
  }

  private def isFileReallyEmpty(filename: String, length: Long): Boolean = if (filename.endsWith(".gz")) {
    // The maximum empty compressed GZ file seems to be around 25 bytes
    // We do this to avoid actually opening a stream on the file and sampling
    length < 25
  } else {
    length == 0
  }

  def apply(options: Options): Boolean = try {
    val ssh = new JSch()

    // Add the private key (PEM) identity if one is specified
    val identity = options.identity match {
      case Some(id) if id.startsWith("s3://") =>
        (id.stripPrefix("s3://").split('/').toList match {
          case bucket :: key =>
            print(s"Downloading identity from s3://$bucket/${key.mkString("/")}...")

            Try(new AmazonS3Client().getObject(bucket, key.mkString("/"))) match {
              case Success(s3Object) =>
                println("done.")
                Option(s3Object)

              case Failure(e) =>
                println("failed.")
                System.err.println(e.getMessage)
                None
            }

          case _ =>
            System.err.println(s"Unknown identity: $id")
            None
        }).map(_.getObjectContent).map(streamToByteArray)

      case Some(id) =>
        println(s"Loading identity from $id...")
        Option(new File(id)).map(fileToByteArray)

      case None => None
    }

    identity.foreach { id =>
      print(s"Attaching identity...")
      ssh.addIdentity("identity", id, null, null)
      println("done.")
    }

    // Get the default information for user, host and port
    val username = options.username.getOrElse(System.getProperty("user.name"))
    val host = options.host
    val port = options.port.getOrElse(22)

    print(s"Opening ssh session to $username@$host:$port...")

    // Open a secure session
    val session = ssh.getSession(username, host, port)

    println("done")

    // Set password info
    session.setUserInfo(new UserInfo {
      override def promptPassword(s: String): Boolean = options.password.nonEmpty
      override def promptYesNo(s: String): Boolean = true
      override def promptPassphrase(s: String): Boolean = false
      override def showMessage(s: String): Unit = println(s)
      override def getPassword: String = options.password.getOrElse("")
      override def getPassphrase: String = ""
    })

    // Connect the session
    print("Connecting...")
    session.connect()
    println("done.")

    try {
      // Start an SFTP channel
      val channel = session.openChannel("sftp")

      // Connect the channel
      channel.connect()

      try {
        val sftp = channel.asInstanceOf[ChannelSftp]
        import sftp.LsEntry

        options.path.foreach { dir =>
          print(s"Setting remote directory to $dir...")
          sftp.cd(dir)
          println("done.")
        }

        // Change directory to folder where we have permissions to get files
        options.mode match {
          case Some(UploadAction) =>
            // List all of the files in the source folder
            Paths.get(System.getenv("INPUT1_STAGING_DIR")).toFile.listFiles(new FilenameFilter {
              override def accept(dir: File, name: String): Boolean = name != "_SUCCESS" && options.pattern.forall(name.matches)
            }).foreach { file =>
              if (options.skipEmpty && isFileReallyEmpty(file.getName, file.length())) {
                println(s"Skipping ${file.getAbsolutePath} because it is empty.")
              } else {
                print(s"Uploading ${file.getAbsolutePath} -> ${file.getName}...")

                // Upload the file
                sftp.put(file.getAbsolutePath, file.getName)

                println("done.")
              }
            }

            // Upload an empty _SUCCESS file if we are to mark successful jobs
            if (options.markSuccessfulJobs) {
              val stream = sftp.put("_SUCCESS")
              stream.flush()
              stream.close()
            }

          case Some(DownloadAction) =>
            val sinceDate = options.since.map(_.getMillis / 1000)
            val untilDate = options.until.map(_.getMillis / 1000)

            // List all of the files in the source folder
            sftp.ls(options.pattern.getOrElse("*")).asScala.foreach { entry =>
              val lsEntry = entry.asInstanceOf[LsEntry]
              val sourceFilename = lsEntry.getFilename
              val sourceTimestamp = lsEntry.getAttrs.getMTime

              if (options.skipEmpty && isFileReallyEmpty(sourceFilename, lsEntry.getAttrs.getSize)) {
                println(s"Skipping $sourceFilename because it is empty")
              } else if (sinceDate.forall(_ < sourceTimestamp) && untilDate.forall(_ > sourceTimestamp)) {
                val destFilename = Paths.get(System.getenv("OUTPUT1_STAGING_DIR"), sourceFilename)
                  .toAbsolutePath
                  .toString

                // Download the file
                print(s"Downloading $sourceFilename -> $destFilename...")
                sftp.get(sourceFilename, destFilename)
                println("done.")
              } else {
                println(s"Skipping $sourceFilename because it does not meet the time requirements")
              }
            }

            // Mark a successful job
            if (options.markSuccessfulJobs) {
              Paths.get(System.getenv("OUTPUT1_STAGING_DIR"), "_SUCCESS").toFile.createNewFile()
            }

          case _ =>
        }
      } finally {
        channel.disconnect()
      }
    } finally {
      session.disconnect()
    }

    true
  } catch {
    case e: JSchException =>
      System.err.println()
      System.err.println(e.getMessage)
      false
  }

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Options](s"hyperion-sftp-activity") {
      note("Common options:")
      help("help").text("prints this usage text\n")

      opt[String]('H', "host").valueName("HOST").required().action((x, c) => c.copy(host = x))
        .text("Connect to HOST\n")
      opt[Int]('P', "port").valueName("PORT").optional().action((x, c) => c.copy(port = Option(x)))
        .text("Connect to PORT on HOST (default: 22)\n")
      opt[String]('u', "user").valueName("USERNAME").optional().action((x, c) => c.copy(username = Option(x)))
        .text("Connect to the host using USERNAME\n")
      opt[String]('p', "password").valueName("PASSWORD").optional().action((x, c) => c.copy(password = Option(x)))
        .text("Provide PASSWORD for the USER\n")
      opt[String]('i', "identity").valueName("IDENTITY").optional().action((x, c) => c.copy(identity = Option(x)))
        .text("Use the IDENTITY instead of a PASSWORD\n")
      opt[Unit]("skip-empty").optional().action((_, c) => c.copy(skipEmpty = true))
      opt[Unit]("mark-successful-jobs").optional().action((_, c) => c.copy(markSuccessfulJobs = true))
      opt[String]("pattern").valueName("PATTERN").optional().action((x, c) => c.copy(pattern = Option(x)))
        .text("Search for files matching PATTERN to upload/download\n")

      cmd(UploadAction.toString).action((_, c) => c.copy(mode = Option(UploadAction)))
        .text(
          """
            |  Uploads files matching PATTERN from INPUT1_STAGING_DIR to the DESTINATION.
          """.stripMargin)
        .children(
          arg[String]("DESTINATION").optional().action((x, c) => c.copy(path = Option(x)))
            .text("Uploads files to DESTINATION.\n")
        )

      cmd(DownloadAction.toString).action((_, c) => c.copy(mode = Option(DownloadAction)))
        .text(
          """
            |  Downloads files matching PATTERN from SOURCE to OUTPUT1_STAGING_DIR.
          """.stripMargin)
        .children(
          opt[String]("since").valueName("TIMESTAMP").optional().action((x, c) => c.copy(since = Option(new DateTime(x))))
            .text("Download files modified after TIMESTAMP.\n"),
          opt[String]("until").valueName("TIMESTAMP").optional().action((x, c) => c.copy(until = Option(new DateTime(x))))
            .text("Download files modified before TIMESTAMP.\n"),
          arg[String]("SOURCE").optional().action((x, c) => c.copy(path = Option(x)))
            .text("Downloads files from SOURCE.\n")
        )

      checkConfig(_.mode match {
        case Some(UploadAction) => if (!System.getenv().containsKey("INPUT1_STAGING_DIR")) {
          failure("INPUT1_STAGING_DIR must be set")
        } else {
          val path = Paths.get(System.getenv("INPUT1_STAGING_DIR")).toFile
          if (!path.exists()) {
            failure(s"Input path $path does not exist.")
          } else if (!path.isDirectory) {
            failure(s"Input path $path is not a directory.")
          } else if (!path.canRead) {
            failure(s"Input path $path cannot be read.")
          } else {
            success
          }
        }

        case Some(DownloadAction) => if (!System.getenv().containsKey("OUTPUT1_STAGING_DIR")) {
          failure("OUTPUT1_STAGING_DIR must be set")
        } else {
          val path = Paths.get(System.getenv("OUTPUT1_STAGING_DIR")).toFile
          if (!path.exists()) {
            failure(s"Output path $path does not exist.")
          } else if (!path.isDirectory) {
            failure(s"Output path $path is not a directory.")
          } else if (!path.canRead) {
            failure(s"Output path $path cannot be read.")
          } else {
            success
          }
        }

        case _ => failure("command is required")
      })

      checkConfig(_.identity match {
        case Some(id) => if (id.startsWith("s3")) {
          success
        } else {
          val idFile = new File(id)
          if (!idFile.exists()) {
            failure(s"Identity file $id does not exist.")
          } else if (!idFile.isFile) {
            failure(s"Identity file $id is not a normal file.")
          } else if (!idFile.canRead) {
            failure(s"Identity file $id cannot be read.")
          } else {
            success
          }
        }

        case _ => success
      })
    }

    if (!parser.parse(args, Options()).exists { options =>
      options.mode match {
        case Some(direction) =>
          this(options)

        case _ =>
          parser.showUsageAsError
          false
      }
    }) System.exit(3)
  }
}
