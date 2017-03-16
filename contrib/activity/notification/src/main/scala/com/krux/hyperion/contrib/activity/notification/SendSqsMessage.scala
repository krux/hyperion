package com.krux.hyperion.contrib.activity.notification

import scala.collection.JavaConverters._

import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.services.sqs.model.{MessageAttributeValue, SendMessageRequest}
import scopt.OptionParser

object SendSqsMessage {
  case class Options(
    region: Option[String] = None,
    queueUrl: Option[String] = None,
    message: Option[String] = None,
    delaySeconds: Option[Int] = Option(0),
    attributes: Map[String, String] = Map.empty
  )

  def apply(options: Options): Boolean = try {
    // Setup the SQS client
    val sqs = new AmazonSQSClient()
    options.region.map(Regions.fromName).map(Region.getRegion).foreach(sqs.setRegion)

    // Create the request from the options specified
    val request = new SendMessageRequest()
    options.queueUrl.foreach(request.setQueueUrl)
    options.message.foreach(request.setMessageBody)
    options.delaySeconds.foreach(request.setDelaySeconds(_))

    // Add the message attributes if any
    if (options.attributes.nonEmpty) {
      request.setMessageAttributes(options.attributes.flatMap { case (k, v) =>
        k.split(":").toList match {
          case key :: dataType :: Nil => Option(key -> new MessageAttributeValue().withStringValue(v).withDataType(dataType))
          case key :: Nil => Option(key -> new MessageAttributeValue().withStringValue(v).withDataType("String"))
          case _ => None
        }
      }.asJava)
    }

    // Publish the message
    val response = sqs.sendMessage(request)

    // Print out the message-id to output
    println(response.getMessageId)

    true
  } catch {
    case e: Throwable =>
      System.err.println(e.getMessage)
      false
  }

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Options](s"hyperion-notification-sqs-activity") {
      override def showUsageOnError = false

      note(
        """Sends a notification message to a SQS Queue.
        """.stripMargin
      )

      help("help").text("prints this usage text")
      opt[String]("region").valueName("REGION").optional().action((x, c) => c.copy(region = Option(x)))
        .text("Sets the region to REGION")
      opt[String]("queue").valueName("URL").required().action((x, c) => c.copy(queueUrl = Option(x)))
        .text("Sends the message to the given queue URL")
      opt[String]("message").valueName("TEXT").required().action((x, c) => c.copy(message = Option(x)))
        .text("Sends the given TEXT as the message")
      opt[Int]("delay").valueName("SECONDS").optional().action((x, c) => c.copy(delaySeconds = Option(x)))
        .text("Delays sending the message for SECONDS")
      opt[Map[String, String]]("attributes").valueName("k1=v1,k2:type=v2...").action((x, c) => c.copy(attributes = x))
        .text("Sets the messages attributes")
    }

    if (!parser.parse(args, Options()).exists(apply)) {
      parser.showUsageAsError
      System.exit(3)
    }
  }
}
