package com.krux.hyperion.contrib.activity.notification

import java.net.{ HttpURLConnection, URL }

import org.json4s.JsonAST.{ JString, JObject }
import org.json4s.jackson.JsonMethods._
import scopt.OptionParser

object SendSlackMessage {
  case class Options(
    failOnError: Boolean = false,
    webhookUrl: String = "",
    user: Option[String] = None,
    message: Seq[String] = Seq.empty,
    iconEmoji: Option[String] = None,
    channel: Option[String] = None
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
      val message = Seq(
        "icon_emoji" -> options.iconEmoji,
        "channel" -> options.channel,
        "username" -> options.user,
        "text" -> Option(options.message.mkString("\n"))
      ).flatMap {
        case (k, None) => None
        case (k, Some(v)) => Option(k -> JString(v))
      }

      output.write(compact(render(JObject(message: _*))).getBytes)
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
      arg[String]("MESSAGE").required().unbounded().action((x, c) => c.copy(message = c.message :+ x))
        .text("Sends the given MESSAGE")
    }

    if (!parser.parse(args, Options()).exists(apply)) {
      System.exit(3)
    }
  }
}
