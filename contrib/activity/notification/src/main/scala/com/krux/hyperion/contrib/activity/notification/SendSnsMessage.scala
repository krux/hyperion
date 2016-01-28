package com.krux.hyperion.contrib.activity.notification

import scala.collection.JavaConverters._

import com.amazonaws.regions.{ Region, Regions }
import com.amazonaws.services.sns.AmazonSNSClient
import com.amazonaws.services.sns.model.{ MessageAttributeValue, PublishRequest }
import scopt.OptionParser

object SendSnsMessage {
  case class Options(
    region: Option[String] = None,
    topicArn: Option[String] = None,
    message: Option[String] = None,
    subject: Option[String] = None,
    json: Boolean = false,
    attributes: Map[String, String] = Map.empty
  )

  def apply(options: Options): Boolean = try {
    // Setup the SNS client
    val sns: AmazonSNSClient = new AmazonSNSClient()
    options.region.map(Regions.fromName).map(Region.getRegion).foreach(sns.setRegion)

    // Create the request from the options specified
    val request = new PublishRequest()
    options.topicArn.foreach(request.setTopicArn)
    options.message.foreach(request.setMessage)
    options.subject.foreach(request.setSubject)
    if (options.json) request.setMessageStructure("json")

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
    val response = sns.publish(request)

    // Print out the message-id to output
    println(response.getMessageId)

    true
  } catch {
    case e: Throwable =>
      System.err.println(s"${e.getMessage}\n")
      false
  }

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Options](s"hyperion-notification-sns-activity") {
      override def showUsageOnError = false

      note(
        """Sends a notification message to a SNS Topic.
        """.stripMargin
      )

      help("help").text("prints this usage text")
      opt[String]("region").valueName("REGION").optional().action((x, c) => c.copy(region = Option(x)))
        .text("Sets the region to REGION")
      opt[String]("topic-arn").valueName("ARN").required().action((x, c) => c.copy(topicArn = Option(x)))
        .text("Sends the message to the given topic ARN")
      opt[Unit]("json").optional().action((_, c) => c.copy(json = true))
        .text("Interprets the message TEXT as a structured JSON message")
      opt[String]("message").valueName("TEXT").required().action((x, c) => c.copy(message = Option(x)))
        .text("Sends the given TEXT as the message")
      opt[String]("subject").valueName("TEXT").optional().action((x, c) => c.copy(subject = Option(x)))
        .text("Sends the given TEXT as the subject")
      opt[Map[String, String]]("attributes").valueName("k1=v1,k2:type=v2...").action((x, c) => c.copy(attributes = x))
        .text("Sets the messages attributes")
    }

    if (!parser.parse(args, Options()).exists(apply)) {
      parser.showUsageAsError
      System.exit(3)
    }
  }
}
