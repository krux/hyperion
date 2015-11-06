package com.krux.hyperion

import java.io.{PrintStream, File}
import java.time.DayOfWeek
import java.time.format.TextStyle
import java.util.Locale

import scopt.OptionParser
import scopt.Read._

import com.github.nscala_time.time.Imports._
import org.json4s.jackson.JsonMethods._
import com.krux.hyperion.workflow.WorkflowGraphRenderer
import com.krux.hyperion.DataPipelineDef._
import com.krux.hyperion.expression.Duration

trait HyperionCli { this: DataPipelineDef =>

  case class Cli(
    mode: String = "generate",
    activate: Boolean = false,
    force: Boolean = false,
    pipelineId: Option[String] = None,
    customName: Option[String] = None,
    tags: Map[String, Option[String]] = Map.empty,
    schedule: Option[Schedule] = None,
    region: Option[String] = None,
    roleArn: Option[String] = None,
    output: Option[File] = None,
    label: String = "id",
    removeLastNameSegment: Boolean = false,
    includeResources: Boolean = false,
    includeDataNodes: Boolean = false,
    includeDatabases: Boolean = false
  )

  lazy val daysOfWeek = Seq(
    DayOfWeek.MONDAY,
    DayOfWeek.TUESDAY,
    DayOfWeek.WEDNESDAY,
    DayOfWeek.THURSDAY,
    DayOfWeek.FRIDAY,
    DayOfWeek.SATURDAY,
    DayOfWeek.SUNDAY
  ).flatMap { dow =>
    Seq(
      dow.getDisplayName(TextStyle.FULL, Locale.getDefault),
      dow.getDisplayName(TextStyle.SHORT, Locale.getDefault)
    )
  }.map(_.toLowerCase).map(dow => dow -> DayOfWeek.valueOf(dow).getValue).toMap

  lazy val daysOfMonth = (1 to 31).flatMap { dom =>
    Seq(dom.toString -> dom, dom % 10 match {
      case 1 => s"${dom}st" -> dom
      case 2 => s"${dom}nd" -> dom
      case 3 => s"${dom}rd" -> dom
      case _ => s"${dom}th" -> dom
    })
  }.toMap

  implicit val durationRead: scopt.Read[Duration] = reads { x => Duration(x) }

  implicit val dateTimeRead: scopt.Read[DateTime] = reads { x =>
    val dt = x.toLowerCase match {
      case "now" | "today" => DateTime.now
      case "yesterday" => DateTime.yesterday
      case "tomorrow" => DateTime.tomorrow
      case dow if daysOfWeek.keySet contains dow => DateTime.now.withDayOfWeek(daysOfMonth(dow))
      case dom if daysOfMonth.keySet contains dom => DateTime.now.withDayOfMonth(daysOfMonth(dom))
      case d => DateTime.parse(d)
    }

    dt.withZone(DateTimeZone.UTC)
  }

  implicit val scheduleRead: scopt.Read[Schedule] = reads { x =>
    Schedule.cron.startDateTime(dateTimeRead.reads(x))
  }

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Cli](s"hyperion") {
      head("hyperion")

      cmd("generate").action { (_, c) => c.copy(mode = "generate") }
        .children(
          opt[File]('o', "output").valueName("<file>").action { (x, c) => c.copy(output = Option(x)) }
        )

      cmd("graph").action { (_, c) => c.copy(mode = "graph") }
        .children(
          opt[File]('o', "output").valueName("<file>").action { (x, c) => c.copy(output = Option(x)) },
          opt[String]("label").valueName("id|name").action { (x, c) => c.copy(label = x) },
          opt[Unit]("remove-last-name-segment").action { (_, c) => c.copy(removeLastNameSegment = true) },
          opt[Unit]("include-resources").action { (_, c) => c.copy(includeResources = true) },
          opt[Unit]("include-data-nodes").action { (_, c) => c.copy(includeDataNodes = true) },
          opt[Unit]("include-databases").action { (_, c) => c.copy(includeDatabases = true) }
        )

      cmd("create").action { (_, c) => c.copy(mode = "create") }
        .children(
          opt[Unit]("force").action { (_, c) => c.copy(force = true) },
          opt[Unit]("activate").action { (_, c) => c.copy(activate = true) },
          opt[String]('n', "name").valueName("<name>").action { (x, c) => c.copy(customName = Option(x)) },
          opt[String]("region").valueName("<region>").action { (x, c) => c.copy(region = Option(x)) },
          opt[String]("role").valueName("<role-arn>").action { (x, c) => c.copy(roleArn = Option(x)) },
          opt[(String, String)]('t', "tags").valueName("<tag>").action { (x, c) =>
            val tag = x match {
              case (k, "") => (k, None)
              case (k, v) => (k, Option(v))
            }
            c.copy(tags = c.tags + tag)
          } unbounded(),
          opt[Schedule]("start").action { (x, c) => c.copy(schedule = Option(x)) },
          opt[Duration]("every").action { (x, c) => c.copy(schedule = c.schedule.map(_.every(x))) },
          opt[DateTime]("until").action { (x, c) => c.copy(schedule = c.schedule.map(_.until(x))) },
          opt[Int]("times").action { (x, c) => c.copy(schedule = c.schedule.map(_.stopAfter(x))) }
        )

      cmd("delete").action { (_, c) => c.copy(mode = "delete") }
        .children(
          opt[String]('n', "name").valueName("<name>").action { (x, c) => c.copy(customName = Option(x)) },
          opt[String]("region").valueName("<region>").action { (x, c) => c.copy(region = Option(x)) },
          opt[String]("role").valueName("<role-arn>").action { (x, c) => c.copy(roleArn = Option(x)) }
        )

      cmd("activate").action { (_, c) => c.copy(mode = "activate") }
        .children(
          opt[String]('n', "name").valueName("<name>").action { (x, c) => c.copy(customName = Option(x)) },
          opt[String]("region").valueName("<region>").action { (x, c) => c.copy(region = Option(x)) },
          opt[String]("role").valueName("<role-arn>").action { (x, c) => c.copy(roleArn = Option(x)) }
        )
    }

    parser.parse(args, Cli()).map { cli =>
      val pipelineDef = DataPipelineDefWrapper(this)
        .withTags(cli.tags)
        .withName(cli.customName.getOrElse(this.pipelineName))
        .withSchedule(cli.schedule.getOrElse(this.schedule))

      val awsClient = new HyperionAwsClient(cli.region, cli.roleArn)
      val awsClientForPipeline = awsClient.ForPipelineDef(pipelineDef)

      cli.mode match {
        case "generate" =>
          cli.output.map(f => new PrintStream(f)).getOrElse(System.out).println(pretty(render(pipelineDef)))
          0

        case "graph" =>
          val renderer = WorkflowGraphRenderer(pipelineDef, cli.removeLastNameSegment, cli.label,
            cli.includeResources, cli.includeDataNodes, cli.includeDatabases)
          cli.output.map(f => new PrintStream(f)).getOrElse(System.out).println(renderer.render())
          0

        case "create" =>
          awsClientForPipeline.createPipeline(cli.force) match {
            case Some(id) if cli.activate =>
              if (awsClient.ForPipelineId(id).activatePipelineById()) 0 else 3

            case None =>
              3

            case _ =>
              0
          }

        case "delete" =>
          if (awsClientForPipeline.deletePipeline()) 0 else 3

        case "activate" =>
          if (awsClientForPipeline.activatePipeline()) 0 else 3

        case _ =>
          parser.showUsageAsError
          3
      }
    }.foreach(System.exit)
  }

}

