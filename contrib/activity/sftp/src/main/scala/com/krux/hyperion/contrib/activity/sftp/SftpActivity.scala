package com.krux.hyperion.contrib.activity.sftp

import java.io.{File, FilenameFilter}
import java.nio.file.Paths
import scala.collection.JavaConverters._

import com.jcraft.jsch.{UserInfo, ChannelSftp, JSch}
import scopt.OptionParser

object SftpActivity {
  case class Options(
    mode: Option[String] = None,
    username: String = "",
    password: Option[String] = None,
    identity: Option[String] = None,
    host: String = "",
    port: Option[Int] = None,
    source: Option[String] = None,
    destination: Option[String] = None,
    pattern: Option[String] = None
  )

  def apply(options: Options): Unit = {
    val ssh = new JSch()

    // Add the private key (PEM) identity
    options.identity.foreach(identity => ssh.addIdentity("identity", null/*TODO identity*/, null, null))

    // Open a secure session
    val session = options.port
      .map(port => ssh.getSession(options.username, options.host, port))
      .getOrElse(ssh.getSession(options.username, options.host))

    // Set password info
    session.setUserInfo(new UserInfo {
      override def promptPassword(s: String): Boolean = false
      override def promptYesNo(s: String): Boolean = true
      override def showMessage(s: String): Unit = {}
      override def getPassword: String = options.password.getOrElse("")
      override def promptPassphrase(s: String): Boolean = false
      override def getPassphrase: String = ""
    })

    // Connect the session
    session.connect()

    try {
      // Start an SFTP channel
      val channel = session.openChannel("sftp")

      // Connect the channel
      channel.connect()

      try {
        val sftp = channel.asInstanceOf[ChannelSftp]

        // Change directory to folder where we have permissions to get files
        options.mode match {
          case Some("upload") =>
            options.destination.foreach(sftp.cd)

            // List all of the files in the source folder
            Paths.get(options.source.getOrElse(".")).toFile.listFiles(new FilenameFilter {
              override def accept(dir: File, name: String): Boolean = options.pattern.map(name.matches).getOrElse(true)
            }).foreach { file =>
              try {
                // Upload the file
                sftp.put(file.getAbsolutePath, file.getName)
              } catch {
                case e: Throwable => {
                  println(s"EXCEPTION: ${e.getMessage}")
                  System.exit(3)
                }
              }
            }

          case Some("download") =>
            options.source.foreach(sftp.cd)

            // List all of the files in the source folder
            import sftp.LsEntry

            sftp.ls(options.pattern.getOrElse("*")).asScala.foreach { entry =>
              val sourceFilename = entry.asInstanceOf[LsEntry].getFilename
              val destFilename = Paths.get(options.destination.getOrElse("."), sourceFilename).toAbsolutePath.toString

              try {
                // Download the file
                sftp.get(sourceFilename, destFilename)
              } catch {
                case ex: Throwable =>
                  println(s"EXCEPTION: ${ex.getMessage}")
                  System.exit(3)
              }
            }

          case _ =>
        }
      } finally {
        channel.disconnect()
      }
    } finally {
      session.disconnect()
    }
  }

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Options](s"hyperion-sftp-activity") {
      head("Hyperion SFTP Download/Upload Activity")

      opt[String]('u', "user").valueName("<username>").action { (x, c) => c.copy(username = x) }.required()
      opt[String]('p', "password").valueName("<password>").action { (x, c) => c.copy(password = Option(x)) }.optional()
      opt[String]('i', "identity").valueName("<identity>").action { (x, c) => c.copy(identity = Option(x)) }.optional()
      opt[String]('H', "host").valueName("<host>").action { (x, c) => c.copy(host = x) }.required()
      opt[Int]('P', "port").valueName("<port>").action { (x, c) => c.copy(port = Option(x)) }.required()
      opt[String]("source").valueName("<source>").action { (x, c) => c.copy(source = Option(x)) }.optional()
      opt[String]("destination").valueName("<destination>").action { (x, c) => c.copy(destination = Option(x)) }.optional()
      opt[String]("pattern").valueName("<pattern>").action { (x, c) => c.copy(pattern = Option(x)) }.optional()

      cmd("upload").action { (_, c) => c.copy(mode = Option("upload")) }.text(
        """
          |    Uploads files matching the pattern from the source directory to the destination directory.
        """.stripMargin)

      cmd("download").action { (_, c) => c.copy(mode = Option("download")) }.text(
        """
          |    Downloads files matching the pattern from the source directory to the destination directory.
        """.stripMargin)
    }

    parser.parse(args, Options()).foreach { options =>
      options.mode match {
        case Some(direction) => this(options)
        case _ => parser.showUsageAsError
      }
    }
  }
}
