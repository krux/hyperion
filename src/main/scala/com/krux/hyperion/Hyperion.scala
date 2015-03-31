package com.krux.hyperion

import scopt.OptionParser
import org.json4s.jackson.JsonMethods._
import com.amazonaws.services.datapipeline._
import com.amazonaws.services.datapipeline.model._
import scala.collection.JavaConversions._
import com.krux.hyperion.DataPipelineDef._

/**
 * The cli that generate JSON for given pipeline definition.
 */
object Hyperion {

  implicit val datapipelinDefRead: scopt.Read[DataPipelineDef] = {

    def newDataPipelineDef(c: String): DataPipelineDef =
      Class.forName(c).newInstance.asInstanceOf[DataPipelineDef]

    scopt.Read.reads(className => newDataPipelineDef(className))
  }

  case class Config(
    pipelineClass: Option[DataPipelineDef] = None,
    mode: Option[String] = None,
    activate: Boolean = false,
    force: Boolean = false,
    pipelineId: Option[String] = None,
    name: Option[String] = None
  )

  lazy val client = new DataPipelineClient()

  def getExistingPipeline(name: String): Option[String] = {
    var marker = ""
    while (true) {
      val result = client.listPipelines(new ListPipelinesRequest().withMarker(marker))
      result.getPipelineIdList.filter(_.getName.equals(name)).map(_.getId).foreach(x => return Some(x))
      marker = result.getMarker
      if (!result.getHasMoreResults) return None
    }
    None
  }

  def deletePipeline(pipelineId: String): Unit = {
    println(s"Deleting pipeline $pipelineId")
    client.deletePipeline(new DeletePipelineRequest().withPipelineId(pipelineId))
  }

  def createPipeline(name: String, pipelineDefinition: DataPipelineDef, force: Boolean): Option[String] = {
    println(s"Creating pipeline $name")
    val pipelineId = client.createPipeline(new CreatePipelineRequest().withUniqueId(name).withName(name)).getPipelineId
    println(s"Pipeline created: $pipelineId")

    println("Uploading pipeline definition")
    val putDefinitionResult = client.putPipelineDefinition(new PutPipelineDefinitionRequest()
      .withPipelineId(pipelineId)
      .withPipelineObjects(pipelineDefinition:Seq[PipelineObject])
      .withParameterObjects(pipelineDefinition:Seq[ParameterObject]))

    // Figure out if that worked.
    if (putDefinitionResult.getErrored) {
      println("Failed to create pipeline")
      if (force) {
        deletePipeline(pipelineId)
        return createPipeline(name, pipelineDefinition, false)
      } else {
        println("Use --force to force pipeline creation")
      }
    } else {
      if (putDefinitionResult.getValidationErrors.isEmpty() && putDefinitionResult.getValidationWarnings.isEmpty()) {
        println("Successfully created pipeline")
      } else {
        println("Successful with warnings")
      }
    }

    // Collect and print any warnings/errors
    val warningsAndErrors = putDefinitionResult.getValidationErrors.flatMap(_.getErrors().map("ERROR" -> _)) ++
      putDefinitionResult.getValidationWarnings.flatMap(_.getWarnings().map("WARNING" -> _))
    warningsAndErrors.foreach { case (level, msg) => println(s"$level: $msg") }

    // Finally, return the pipeline ID
    if (!putDefinitionResult.getErrored) Some(pipelineId) else None
  }

  def activatePipeline(pipelineId: String): Unit = {
    println(s"Activating pipeline $pipelineId")
    client.activatePipeline(new ActivatePipelineRequest().withPipelineId(pipelineId))
  }

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Config]("hyperion") {
      head("hyperion", "0.x")

      cmd("generate") action { (_, c) => c.copy(mode = Some("generate")) } children(
        arg[DataPipelineDef]("<pipeline class>").action { (x, c) => c.copy(pipelineClass = Some(x)) }
      )

      cmd("create") action { (_, c) => c.copy(mode = Some("create")) } children(
        opt[Unit]("activate") action { (_, c) => c.copy(activate = true) },
        opt[Unit]("force") action { (_, c) => c.copy(force = true) },
        opt[String]("name") abbr("n") valueName("<name>") action { (x, c) => c.copy(name = Some(x)) },
        arg[DataPipelineDef]("<pipeline class>").action { (x, c) => c.copy(pipelineClass = Some(x)) }
      )

      cmd("activate") action { (_, c) => c.copy(activate = true) } children(
        arg[String]("<pipeline id>").action { (x, c) => c.copy(pipelineId = Some(x)) }
      )
    }

    parser.parse(args, Config()).foreach { config =>
      config.mode match {
        case Some("generate") =>
          config.pipelineClass.foreach(pipeline => println(pretty(render(pipeline))))

        case Some("create") =>
          config.pipelineClass.foreach { pipeline =>
            val pipelineId = createPipeline(config.name.getOrElse(pipeline.getClass.getName),
              pipeline, config.force)
            if (config.activate) pipelineId.map(activatePipeline)
          }

        case None if config.activate =>
          config.pipelineId.foreach(activatePipeline)

        case None => parser.showUsageAsError
      }
    }
  }

}
