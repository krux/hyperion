package com.krux.hyperion.contrib.activity.notification

import java.net.{ HttpURLConnection, URL }

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scopt.OptionParser

object SendFlowdockMessage {
  case class Options(
    failOnError: Boolean = false,
    apiKey: String = "",
    message: String = "",
    user: String = "hyperion",
    tags: Seq[String] = Seq.empty
  )

  def apply(options: Options): Boolean = try {
    // Setup the connection
    val connection = new URL(s"https://api.flowdock.com/v1/messages/chat/${options.apiKey}")
      .openConnection().asInstanceOf[HttpURLConnection]
    connection.setDoOutput(true)
    connection.setRequestProperty("Content-Type", "application/json")
    connection.setRequestProperty("Accept", "application/json")

    // Write the message
    val output = connection.getOutputStream
    try {
      output.write(compact(render(("event" -> "message") ~
        ("external_user_name" -> options.user) ~
        ("content" -> options.message) ~
        ("tags" -> options.tags))).getBytes)
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
    val parser = new OptionParser[Options](s"hyperion-notification-flowdock-activity") {
      override def showUsageOnError = true

      note("Sends a notification message to a Flowdock flow.")
      help("help").text("prints this usage text")
      opt[Unit]("fail-on-error").optional().action((_, c) => c.copy(failOnError = true))
      opt[String]("api-key").valueName("KEY").required().action((x, c) => c.copy(apiKey = x))
        .text("Sends the given TEXT as the subject")
      opt[String]("user").valueName("NAME").optional().action((x, c) => c.copy(user = x))
        .text("Sends the message as the user with NAME")
      opt[Seq[String]]("tags").valueName("TAG1,TAG2").optional().action((x, c) => c.copy(tags = x))
        .text("Adds the tags to the message")
      arg[String]("MESSAGE").required().unbounded().action((x, c) => c.copy(message = s"${c.message} $x"))
        .text("Sends the given MESSAGE")
    }

    if (!parser.parse(args, Options()).exists(apply)) {
      System.exit(3)
    }
  }
}
