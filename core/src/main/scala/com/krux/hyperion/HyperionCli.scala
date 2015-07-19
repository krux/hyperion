package com.krux.hyperion

import scopt.OptionParser
import org.json4s.jackson.JsonMethods._
import com.krux.hyperion.DataPipelineDef._

trait HyperionCli {

  def pipelineDef: DataPipelineDef

  case class Cli(
    mode: String = "generate",
    activate: Boolean = false,
    force: Boolean = false,
    pipelineId: Option[String] = None,
    customName: Option[String] = None,
    region: Option[String] = None,
    roleArn: Option[String] = None,
    tags: Map[String, Option[String]] = Map()
  )

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Cli](s"hyperion") {
      head("hyperion")
      cmd("generate").action { (_, c) => c.copy(mode = "generate") }
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
          } unbounded()
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

    parser.parse(args, Cli()).foreach { cli =>
      val awsClient = new HyperionAwsClient(cli.region, cli.roleArn)
      val awsClientForPipeline = awsClient.ForPipelineDef(pipelineDef, cli.customName)

      cli.mode match {
        case "generate" =>
          println(pretty(render(pipelineDef)))

        case "create" =>
          val pipelineId = awsClientForPipeline.createPipeline(cli.force, cli.tags)
          if (cli.activate) pipelineId.map(awsClient.ForPipelineId).foreach(_.activatePipelineById())

        case "delete" =>
          awsClientForPipeline.deletePipeline()

        case "activate" =>
          awsClientForPipeline.activatePipeline()

        case _ =>
          parser.showUsageAsError
      }
    }
  }

}
