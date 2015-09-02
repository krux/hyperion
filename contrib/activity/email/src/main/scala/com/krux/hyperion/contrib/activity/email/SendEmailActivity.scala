package com.krux.hyperion.contrib.activity.email

import java.io.{File, FilenameFilter}
import java.nio.file.{Files, Paths}
import javax.activation.{DataHandler, FileDataSource}
import javax.mail.Message.RecipientType
import javax.mail.internet.{MimeMultipart, MimeBodyPart, InternetAddress, MimeMessage}
import javax.mail._

import scopt.OptionParser

object SendEmailActivity {
  case class Options(
    host: Option[String] = None,
    port: Option[Int] = None,
    username: Option[String] = None,
    password: Option[String] = None,
    from: Option[String] = None,
    to: Seq[String] = Seq.empty,
    cc: Seq[String] = Seq.empty,
    bcc: Seq[String] = Seq.empty,
    subject: Option[String] = None,
    body: Option[String] = None,
    starttls: Boolean = false,
    debug: Boolean = false
  )

  def apply(options: Options): Boolean = {
    // Set the SMTP properties
    val props = System.getProperties
    options.host.foreach(host => props.put("mail.smtp.host", host))
    options.port.foreach(port => props.put("mail.smtp.port", port.toString))
    options.username.foreach(username => props.put("mail.smtp.user", username))
    options.password.foreach(password => props.put("mail.smtp.password", password))
    if (options.username.nonEmpty || options.password.nonEmpty) {
      println("Enabling auth")
      props.put("mail.smtp.auth", "true")
    }
    props.put("mail.smtp.starttls.enable", options.starttls.toString)
    props.put("mail.smtp.debug", options.debug.toString)

    // Open a session using the properties
    print("Opening session...")
    val session = Session.getDefaultInstance(props, new Authenticator {
      override def getPasswordAuthentication: PasswordAuthentication = {
        println("Authenticating...")
        new PasswordAuthentication(options.username.get, options.password.get)
      }
    })
    println("done.")

    try {
      println("Creating message...")

      // Create a new message
      val message = new MimeMessage(session)
      val multipart = new MimeMultipart()

      // Set the from address
      options.from.orElse(options.username).flatMap(from => InternetAddress.parse(from, false).toSeq.headOption).foreach(from => message.setFrom(from))

      // Add the primary recipients
      options.to match {
        case Seq() =>
        case recipients => message.setRecipients(RecipientType.TO, InternetAddress.parse(recipients.mkString(","), false).asInstanceOf[Array[Address]])
      }

      // Add the carbon copy recipients
      options.cc match {
        case Seq() =>
        case recipients => message.setRecipients(RecipientType.CC, InternetAddress.parse(recipients.mkString(","), false).asInstanceOf[Array[Address]])
      }

      // Add the blind carbon copy recipients
      options.bcc match {
        case Seq() =>
        case recipients => message.setRecipients(RecipientType.BCC, InternetAddress.parse(recipients.mkString(","), false).asInstanceOf[Array[Address]])
      }

      // Set the subject
      options.subject.foreach(subject => message.setSubject(subject))

      // Set the body text
      options.body.foreach { body =>
        val part = new MimeBodyPart()
        part.setText(body)
        multipart.addBodyPart(part)
      }

      // Add the attachments
      println("Checking for attachments...")
      (1 until 11).map(n => s"INPUT${n}_STAGING_DIR").flatMap(v => Option(System.getenv(v))).map(n => Paths.get(n).toFile).foreach { f =>
        f.listFiles(new FilenameFilter {
          override def accept(dir: File, name: String): Boolean = Files.size(Paths.get(dir.getAbsolutePath, name)) > 0
        }).foreach { file =>
          println(s"Adding attachment $file")
          val part = new MimeBodyPart()
          part.setDataHandler(new DataHandler(new FileDataSource(file)))
          part.setFileName(file.getName)
          multipart.addBodyPart(part)
        }
      }

      message.setContent(multipart)

      // Get the SMTP transport
      val transport = session.getTransport("smtp")
      try {
        // Connect to the SMTP server
        println("Connecting...")
        transport.connect()

        // Send the message
        print("Sending...")
        transport.sendMessage(message, message.getAllRecipients)
        println("done.")
      } finally {
        // Close the SMTP connection
        transport.close()
      }
      true
    } catch {
      case e: MessagingException =>
        System.err.println()
        System.err.println(e.getMessage)
        false
    }
  }

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Options](s"hyperion-email-activity") {
      note("Common options:")
      help("help").text("prints this usage text\n")

      opt[String]('H', "host").valueName("SERVER").optional().action((x, c) => c.copy(host = Option(x)))
        .text("Connects to SERVER to send message (default: localhost)\n")
      opt[Int]('P', "port").valueName("PORT").optional().action((x, c) => c.copy(port = Option(x)))
        .text("Connects to PORT on SERVER to send message (default: 25)\n")
      opt[String]('u', "username").valueName("USERNAME").optional().action((x, c) => c.copy(username = Option(x)))
        .text("Uses USERNAME to authenticate to SERVER\n")
      opt[String]('p', "password").valueName("PASSWORD").optional().action((x, c) => c.copy(password = Option(x)))
        .text("Uses PASSWORD to authenticate to SERVER\n")
      opt[String]("from").valueName("ADDRESS").required().action((x, c) => c.copy(from = Option(x)))
        .text("Sets the From/Sender to ADDRESS\n")
      opt[Seq[String]]("to").valueName("ADDRESS").required().unbounded().action((x, c) => c.copy(to = c.to ++ x))
        .text("Adds ADDRESS as a To recipient\n")
      opt[Seq[String]]("cc").valueName("ADDRESS").optional().unbounded().action((x, c) => c.copy(cc = c.cc ++ x))
        .text("Adds ADDRESS as a CC recipient\n")
      opt[Seq[String]]("bcc").valueName("ADDRESS").optional().unbounded().action((x, c) => c.copy(bcc = c.bcc ++ x))
        .text("Adds ADDRESS as a BCC recipient\n")
      opt[String]('s', "subject").valueName("SUBJECT").required().action((x, c) => c.copy(subject = Option(x)))
        .text("Sets the message subject to SUBJECT\n")
      opt[String]('B', "body").valueName("MESSAGE").required().action((x, c) => c.copy(body = Option(x)))
        .text("Sets the message body to MESSAGE\n")
      opt[Unit]("starttls").optional().action((_, c) => c.copy(starttls = true))
        .text("Start TLS when connecting to SERVER\n")
      opt[Unit]("debug").optional().action((_, c) => c.copy(debug = true))
        .text("Enables debug messages\n")
    }

    if (!parser.parse(args, Options()).exists(apply)) System.exit(3)
  }
}
