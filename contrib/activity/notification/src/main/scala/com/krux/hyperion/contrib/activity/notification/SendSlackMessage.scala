package com.krux.hyperion.contrib.activity.notification

import java.net.{HttpURLConnection, URL}

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import scopt.OptionParser

object SendSlackMessage {

  implicit val formats = DefaultFormats

  case class SlackAttachment(
    title: Option[String] = None,
    text: String,
    fallback: String,
    color: String
  )

  case class SlackMessage(
     user: Option[String] = None,
     iconEmoji: Option[String] = None,
     channel: Option[String] = None,
     text: String = "",
     attachments: List[SlackAttachment] = List.empty
   )

  case class Options(
    failOnError: Boolean = false,
    webhookUrl: String = "",
    user: Option[String] = None,
    message: Seq[String] = Seq.empty,
    iconEmoji: Option[String] = None,
    channel: Option[String] = None,
    useAttachment: Boolean = false,
    title: Option[String] = None,
    color: Option[String] = None
  )

  def apply(options: Options): Boolean = try {
    // Setup the connection
    val connection = new URL(options.webhookUrl).openConnection().asInstanceOf[HttpURLConnection]
    connection.setDoOutput(true)
    connection.setRequestProperty("Content-Type", "application/json")
    connection.setRequestProperty("Accept", "application/json")

    // Write the message
    val output = connection.getOutputStream
    try {
      // Build new line separated user defined list of messages
      val text = options.message.mkString("\n")
      // Build either plain slack message or with attachments
      val slackMessage = {
        buildSlackMessage(options, text)
      }
      // Build Json string of slack message and push to slack url
      val finalMessage = write(slackMessage)
      output.write(finalMessage.getBytes)
    } finally {
      output.close()
    }
    // Check the response code
    connection.getResponseCode == 200 || !options.failOnError
  } catch {
    case e: Throwable =>
      System.err.println(e.toString)
      !options.failOnError
  }

  private def buildSlackMessage(options: Options, text: String) = {
    if (options.useAttachment) {
      val slackAttachment = List(SlackAttachment(
        title = options.title,
        text = text,
        fallback = text,
        color = options.color.getOrElse("good")
      ))
      SlackMessage(
        user = options.user,
        iconEmoji = options.iconEmoji,
        channel = options.channel,
        attachments = slackAttachment
      )
    }
    else {
      SlackMessage(
        user = options.user,
        iconEmoji = options.iconEmoji,
        channel = options.channel,
        text = text
      )
    }
  }

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Options](s"hyperion-notification-slack-activity") {
      override def showUsageOnError = true

      note("Sends a notification message to a Slack incoming webhook.")
      help("help").text("prints this usage text")
      opt[Unit]("fail-on-error").optional().action((_, c) => c.copy(failOnError = true))
        .text("Causes the activity to fail if any error received from the webhook")
      opt[String]("webhook-url").valueName("WEBHOOK").required().action((x, c) => c.copy(webhookUrl = x))
        .text("Sends the message to the given WEBHOOK url")
      opt[String]("user").valueName("NAME").optional().action((x, c) => c.copy(user = Option(x)))
        .text("Sends the message as the user with NAME")
      opt[String]("emoji").valueName("EMOJI").optional().action((x, c) => c.copy(iconEmoji = Option(x)))
        .text("Use EMOJI for the icon")
      opt[String]("to").valueName("CHANNEL or USERNAME").optional().action((x, c) => c.copy(channel = Option(x)))
        .text("Sends the message to #CHANNEL or @USERNAME")
      opt[Unit]("useAttachment").optional().action((_, c) => c.copy(useAttachment = true))
        .text("Causes the message to be posted as an attachment in Slack")
      opt[String]("title").valueName("TITLE").optional().action((x, c) => c.copy(title = Option(x)))
        .text("Sets the attachment title")
      opt[String]("color").valueName("GOOD or WARNING or DANGER").optional().action((x, c) => c.copy(color = Option(x)))
        .text("Sends the attachment in chosen color")
      arg[String]("MESSAGE").required().unbounded().action((x, c) => c.copy(message = c.message :+ x))
        .text("Sends the given MESSAGE")
    }

    if (!parser.parse(args, Options()).exists(apply)) {
      System.exit(3)
    }
  }
}
