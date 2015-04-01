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
    customName: Option[String] = None
  )

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Cli](s"hyperion") {
      head("hyperion")
      cmd("generate").action { (_, c) => c.copy(mode = "generate") }
      cmd("create").action { (_, c) => c.copy(mode = "create") }
        .children(
          opt[Unit]("force").action { (_, c) => c.copy(force = true) },
          opt[Unit]("activate").action { (_, c) => c.copy(activate = true) },
          opt[String]('n', "name").valueName("<name>").action { (x, c) => c.copy(customName = Some(x)) }
        )
      cmd("delete").action { (_, c) => c.copy(mode = "delete") }
      cmd("activate").action { (_, c) => c.copy(mode = "activate") }
    }

    parser.parse(args, Cli()).foreach { cli =>
      lazy val awsClient = new HyperionAwsClient(pipelineDef, cli.customName)
      cli.mode match {
        case "generate" =>
          println(pretty(render(pipelineDef)))
        case "create" =>
          val pipelineId = awsClient.createPipeline(cli.force)
          if (cli.activate) pipelineId.foreach(HyperionAwsClient.activatePipelineById)
        case "delete" =>
          awsClient.deletePipeline()
        case "activate" =>
          awsClient.activatePipeline()
        case _ =>
          parser.showUsageAsError
      }
    }
  }

}
