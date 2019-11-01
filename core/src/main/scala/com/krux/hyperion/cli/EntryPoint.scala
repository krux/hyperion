package com.krux.hyperion.cli

import java.io.File

import com.github.nscala_time.time.Imports._
import com.krux.hyperion._
import com.krux.hyperion.cli.Reads._
import com.krux.hyperion.expression.Duration
import scopt.OptionParser

/**
  * EntryPoint is the main entrypoint for the CLI.
  */
case class EntryPoint(pipeline: DataPipelineDefGroup) {

  def getRecurringSchedule(c: Options, pipeline: DataPipelineDefGroup): RecurringSchedule =
    c.schedule.getOrElse(pipeline.schedule) match {
      case s: RecurringSchedule => s
      case _ => Schedule.cron
    }

  private val parser = new OptionParser[Options](s"hyperion") {
    head("hyperion", s"${BuildInfo.version} (${BuildInfo.scalaVersion})")
    help("help").hidden
    version("version").hidden

    note("Hyperion CLI provides tools to generate, graph and generate Data Pipelines.\n")

    cmd("generate").action { (_, c) => c.copy(action = GenerateAction) }
      .text(
        """
          |  Generates a JSON representation of a DataPipeline.  Useful for manually creating a
          |  DataPipeline via the AWS Console or for manually inspecting the pipeline generated.
        """.stripMargin)
      .children(
        opt[File]('o', "output").valueName("PATH").action { (x, c) => c.copy(output = Option(x)) }
          .text(
            """
              |     If specified, the pipeline will be generated in the specified PATH.  Otherwise,
              |     the pipeline will be written to standard output.
            """.stripMargin),
        opt[Map[String, String]]("params").valueName("KWARGS (e.g. k1=v1,k2=v2)")
          .action { (x, c) => c.copy(params = c.params ++ x) }
          .text(
            """
              |     If specified, the pipeline will override the parameters specified in the pipeline
              |     definition.
            """.stripMargin)
          .unbounded(),
        opt[(String, String)]("param").valueName("key=value")
          .action { (x, c) => c.copy(params = c.params + x)}
          .text(
            """
              |    If specified, the pipeline will override the parameter specified in the pipeline
              |    definition.
            """.stripMargin
          )
          .unbounded()
      )

    cmd("graph").action { (_, c) => c.copy(action = GraphAction) }
      .text(
        """
          |  Generates a DOT representation of a DataPipeline.  Useful for visualizing the
          |  DataPipeline via GraphViz toolset.
        """.stripMargin)
      .children(
        opt[File]('o', "output").valueName("PATH").action { (x, c) => c.copy(output = Option(x)) }
          .text(
            """
              |     If specified, the pipeline will be generated in the specified PATH.  Otherwise,
              |     the pipeline will be written to standard output.
            """.stripMargin),
        opt[String]("label").valueName("PROPERTY").action { (x, c) => c.copy(label = x) }
          .text(
            """
              |     If specified, the given PROPERTY will be used as the label.  (Default: id).
            """.stripMargin),
        opt[Unit]("remove-last-name-segment").action { (_, c) => c.copy(removeLastNameSegment = true) }
          .text(
            """
              |     If specified, the portion after the last _ character in the label will be removed.
            """.stripMargin),
        opt[Unit]("include-resources").action { (_, c) => c.copy(includeResources = true) }
          .text(
            """
              |     If specified, resources will be included in the output visualization.
            """.stripMargin),
        opt[Unit]("include-data-nodes").action { (_, c) => c.copy(includeDataNodes = true) }
          .text(
            """
              |     If specified, data nodes will be included in the output visualization.
            """.stripMargin),
        opt[Unit]("include-databases").action { (_, c) => c.copy(includeDatabases = true) }
          .text(
            """
              |     If specified, databases will be included in the output visualization.
            """.stripMargin)
      )

    cmd("create").action { (_, c) => c.copy(action = CreateAction) }
      .text(
        """
          |  Creates a DataPipeline by using the AWS SDK.
        """.stripMargin)
      .children(
        opt[Unit]("no-check").action { (_, c) => c.copy(checkExistence = false) }
          .text(
            """
              |     If specified, the existence of the pipeline will not be checked before creating.
            """.stripMargin),
        opt[Unit]("force").action { (_, c) => c.copy(force = true) }
          .text(
            """
              |     If specified, and the pipeline already exists, the existing pipeline will be
              |     deleted before the new pipeline is created.
            """.stripMargin),
        opt[Unit]("activate").action { (_, c) => c.copy(activate = true) }
          .text(
            """
              |     If specified, the newly created pipeline will be activated.
            """.stripMargin),
        opt[String]('n', "name").valueName("NAME").action { (x, c) => c.copy(customName = Option(x)) }
          .text(
            """
              |     If specified, NAME will be used for the pipeline name instead of the name specified
              |     in the pipeline definition.
            """.stripMargin),
        opt[String]("region").valueName("REGION").action { (x, c) => c.copy(region = Option(x)) }
          .text(
            """
              |     If specified, the pipeline will be created in REGION (default: us-east-1).
            """.stripMargin),
        opt[String]("role").valueName("ARN").action { (x, c) => c.copy(roleArn = Option(x)) }
          .text(
            """
              |     If specified, the ARN role will be assumed when creating the pipeline.
            """.stripMargin),
        opt[(String, String)]('t', "tags").valueName("TAG").action { (x, c) =>
          val tag = x match {
            case (k, "") => (k, None)
            case (k, v) => (k, Option(v))
          }
          c.copy(tags = c.tags + tag)
        }.unbounded()
          .text(
            """
              |     If specified, the TAG will be added to the tags specified in the pipeline definition.
            """.stripMargin),
        opt[DateTime]("start").valueName("DATE")
          .action { (x, c) =>
            c.copy(schedule = Option(getRecurringSchedule(c, pipeline).startDateTime(x)))
          }
          .text(
            """
              |     If specified, the pipeline will start on DATE, overriding the schedule specified in
              |     the pipeline definition.  The following special values are recognized: now, today,
              |     yesterday, tomorrow, the days of the week and ordinal numbers (e.g., 1st, 2nd) for days
              |     of the month.
            """.stripMargin),
        opt[Duration]("every").valueName("PERIOD")
          .action { (x, c) =>
            c.copy(schedule = Option(getRecurringSchedule(c, pipeline).every(x)))
          }
          .text(
            """
              |     If specified, the pipeline will execute every PERIOD (ex: "1 day").
            """.stripMargin),
        opt[DateTime]("until").valueName("DATE")
          .action { (x, c) =>
            c.copy(schedule = Option(getRecurringSchedule(c, pipeline).until(x)))
          }
          .text(
            """
              |     If specified, the pipeline will stop on DATE.
            """.stripMargin),
        opt[Int]("times").valueName("N")
          .action { (x, c) =>
            c.copy(schedule = Option(getRecurringSchedule(c, pipeline).stopAfter(x)))
          }
          .text(
            """
              |     If specified, the pipeline will stop after executing N times.
            """.stripMargin),
        opt[Map[String, String]]("params").valueName("KWARGS (e.g. k1=v1,k2=v2)")
          .action { (x, c) => c.copy(params = c.params ++ x) }
          .text(
            """
              |     If specified, the pipeline will override the parameters specified in the pipeline
              |     definition.
            """.stripMargin)
          .unbounded(),
        opt[(String, String)]("param").valueName("key=value")
          .action { (x, c) => c.copy(params = c.params + x)}
          .text(
            """
              |    If specified, the pipeline will override the parameter specified in the pipeline
              |    definition.
            """.stripMargin
          )
          .unbounded()
      )

    cmd("delete").action { (_, c) => c.copy(action = DeleteAction) }
      .text(
        """
          |  Deletes the pipeline using the AWS SDK.
        """.stripMargin)
      .children(
        opt[String]('n', "name").valueName("NAME").action { (x, c) => c.copy(customName = Option(x)) }
          .text(
            """
              |     If specified, NAME will be used for the pipeline name instead of the name specified
              |     in the pipeline definition.  This name is used to look up the pipeline.
            """.stripMargin),
        opt[String]("region").valueName("REGION").action { (x, c) => c.copy(region = Option(x)) }
          .text(
            """
              |     If specified, the pipeline will be created in REGION.
            """.stripMargin),
        opt[String]("role").valueName("ARN").action { (x, c) => c.copy(roleArn = Option(x)) }
          .text(
            """
              |     If specified, the ARN role will be assumed when creating the pipeline.
            """.stripMargin)
      )

    cmd("activate").action { (_, c) => c.copy(action = ActivateAction) }
      .text(
        """
          |  Activates the pipeline using the AWS SDK.
        """.stripMargin)
      .children(
        opt[String]('n', "name").valueName("NAME").action { (x, c) => c.copy(customName = Option(x)) }
          .text(
            """
              |     If specified, NAME will be used for the pipeline name instead of the name specified
              |     in the pipeline definition.  This name is used to look up the pipeline.
            """.stripMargin),
        opt[String]("region").valueName("REGION").action { (x, c) => c.copy(region = Option(x)) }
          .text(
            """
              |     If specified, the pipeline will be created in REGION.
            """.stripMargin),
        opt[String]("role").valueName("ARN").action { (x, c) => c.copy(roleArn = Option(x)) }
          .text(
            """
              |     If specified, the ARN role will be assumed when creating the pipeline.
            """.stripMargin)
      )
  }

  def run(args: Array[String]): Int = parser.parse(args, Options()).map { cli =>
    val wrappedPipeline = DataPipelineDefGroupWrapper(pipeline)
      .withTags(cli.tags)
      .withName(cli.customName.getOrElse(pipeline.pipelineName))
      .withSchedule(cli.schedule.getOrElse(pipeline.schedule))

    for { (id, value) <- cli.params } wrappedPipeline.setParameterValue(id, value)

    if (cli.action(cli, wrappedPipeline)) 0 else 3
  }.getOrElse(3)

}
