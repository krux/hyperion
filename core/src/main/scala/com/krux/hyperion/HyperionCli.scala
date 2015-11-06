package com.krux.hyperion

import java.io.{PrintStream, File}
import java.time.DayOfWeek
import java.time.format.TextStyle
import java.util.Locale

import com.github.nscala_time.time.Imports.DateTime
import scopt.OptionParser
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
    region: Option[String] = None,
    roleArn: Option[String] = None,
    output: Option[File] = None,
    label: String = "id",
    removeLastNameSegment: Boolean = false,
    includeResources: Boolean = false,
    includeDataNodes: Boolean = false,
    includeDatabases: Boolean = false,
    tags: Map[String, Option[String]] = Map.empty,
    schedule: Option[Schedule] = None
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
  }.map(_.toLowerCase)

  lazy val daysOfMonth = (1 to 31).flatMap { dom =>
    Seq(dom.toString, dom % 10 match {
      case 1 => s"${dom}st"
      case 2 => s"${dom}nd"
      case 3 => s"${dom}rd"
      case _ => s"${dom}th"
    })
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
          opt[String]("start").action { (x, c) => c.copy(schedule = x.toLowerCase match {
            case "now" | "today" => Option(Schedule.cron.startAtActivation)
            case "yesterday" => Option(Schedule.cron.startDateTime(DateTime.yesterday))
            case "tomorrow" => Option(Schedule.cron.startDateTime(DateTime.tomorrow))
            case dow if daysOfWeek contains dow => Option(Schedule.cron.startThisWeekAt(DayOfWeek.valueOf(dow).getValue, 0, 0, 0))
            case dom if daysOfMonth contains dom => Option(Schedule.cron.startThisMonthAt(dom.toInt, 0, 0, 0))
            case _ => Option(Schedule.cron.startDateTime(DateTime.parse(x)))
          }) },
          opt[String]("every").action { (x, c) => c.copy(schedule = c.schedule.map(_.every(Duration(x)))) },
          opt[String]("until").action { (x, c) => x match {
            case "tomorrow" => c.copy(schedule = c.schedule.map(_.until(DateTime.tomorrow)))
            case _ => c.copy(schedule = c.schedule.map(_.until(DateTime.parse(x))))
          } },
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
      val awsClient = new HyperionAwsClient(cli.region, cli.roleArn)
      val awsClientForPipeline = awsClient.ForPipelineDef(this, cli.customName, cli.schedule)

      cli.mode match {
        case "generate" =>
          cli.output.map(f => new PrintStream(f)).getOrElse(System.out).println(pretty(render(this)))
          0

        case "graph" =>
          val renderer = WorkflowGraphRenderer(this, cli.removeLastNameSegment, cli.label,
            cli.includeResources, cli.includeDataNodes, cli.includeDatabases)
          cli.output.map(f => new PrintStream(f)).getOrElse(System.out).println(renderer.render())
          0

        case "create" =>
          awsClientForPipeline.createPipeline(cli.force, cli.tags) match {
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

