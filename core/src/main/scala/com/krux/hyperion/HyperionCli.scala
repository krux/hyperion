package com.krux.hyperion

import java.io.File

import scopt.OptionParser

import com.github.nscala_time.time.Imports._
import com.krux.hyperion.cli._
import com.krux.hyperion.cli.Reads._
import com.krux.hyperion.expression.Duration

trait HyperionCli { this: DataPipelineDef =>

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Cli](s"hyperion") {
      head("hyperion")

      cmd("generate").action { (_, c) => c.copy(action = GenerateAction) }
        .children(
          opt[File]('o', "output").valueName("<file>").action { (x, c) => c.copy(output = Option(x)) }
        )

      cmd("graph").action { (_, c) => c.copy(action = GraphAction) }
        .children(
          opt[File]('o', "output").valueName("<file>").action { (x, c) => c.copy(output = Option(x)) },
          opt[String]("label").valueName("id|name").action { (x, c) => c.copy(label = x) },
          opt[Unit]("remove-last-name-segment").action { (_, c) => c.copy(removeLastNameSegment = true) },
          opt[Unit]("include-resources").action { (_, c) => c.copy(includeResources = true) },
          opt[Unit]("include-data-nodes").action { (_, c) => c.copy(includeDataNodes = true) },
          opt[Unit]("include-databases").action { (_, c) => c.copy(includeDatabases = true) }
        )

      cmd("create").action { (_, c) => c.copy(action = CreateAction) }
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

      cmd("delete").action { (_, c) => c.copy(action = DeleteAction) }
        .children(
          opt[String]('n', "name").valueName("<name>").action { (x, c) => c.copy(customName = Option(x)) },
          opt[String]("region").valueName("<region>").action { (x, c) => c.copy(region = Option(x)) },
          opt[String]("role").valueName("<role-arn>").action { (x, c) => c.copy(roleArn = Option(x)) }
        )

      cmd("activate").action { (_, c) => c.copy(action = ActivateAction) }
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

      if (cli.action.execute(cli, pipelineDef)) 0 else 3
    }.foreach(System.exit)
  }

}

