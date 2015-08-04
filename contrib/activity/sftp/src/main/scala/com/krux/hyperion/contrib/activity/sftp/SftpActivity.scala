package com.krux.hyperion.contrib.activity.sftp

import java.io._
import java.nio.file.Paths
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3}

import scala.collection.JavaConverters._

import com.jcraft.jsch.{JSchException, UserInfo, ChannelSftp, JSch}
import scopt.OptionParser

import scala.util.{Failure, Success, Try}

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
    pattern: Option[String] = None
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
      override def promptPassword(s: String): Boolean = false
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
              override def accept(dir: File, name: String): Boolean = options.pattern.forall(name.matches)
            }).foreach { file =>
              print(s"Uploading ${file.getAbsolutePath} -> ${file.getName}...")
              // Upload the file
              sftp.put(file.getAbsolutePath, file.getName)
              println("done.")
            }

          case Some(DownloadAction) =>
            // List all of the files in the source folder
            sftp.ls(options.pattern.getOrElse("*")).asScala.foreach { entry =>
              val sourceFilename = entry.asInstanceOf[LsEntry].getFilename
              val destFilename = Paths.get(System.getenv("OUTPUT1_STAGING_DIR"), sourceFilename).toAbsolutePath
                .toString

              print(s"Downloading $sourceFilename -> $destFilename...")

              // Download the file
              sftp.get(sourceFilename, destFilename)

              println("done.")
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
