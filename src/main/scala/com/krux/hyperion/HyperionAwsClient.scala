package com.krux.hyperion

import com.amazonaws.services.datapipeline._
import com.amazonaws.services.datapipeline.model._
import com.krux.hyperion.DataPipelineDef._
import scala.collection.JavaConversions._

class HyperionAwsClient(pipelineDef: DataPipelineDef, customName: Option[String] = None) {

  lazy val client = HyperionAwsClient.client

  val pipelineName = customName.getOrElse(pipelineDef.pipelineName)

  def getPipelineId: Option[String] = {
    val idNameToIdPartial = Function.unlift((idName: PipelineIdName) =>
      if (idName.getName == pipelineName) Some(idName.getId) else None
    )
    val ids = client.listPipelines().getPipelineIdList().collect(idNameToIdPartial).toList
    ids match {
      case x :: y :: other =>  // if using hyperion for all datapipeline management, this should never happen
        throw new Exception("Duplicated pipeline name")
      case x :: Nil => Some(x)
      case Nil => None
    }
  }

  def createPipeline(force: Boolean = false): Option[String] = {

    println(s"Creating pipeline $pipelineName")

    getPipelineId match {

      case Some(pipelineId) =>
        println("Pipeline already exists")
        if (force) {
          println("Delete the existing pipline")
          HyperionAwsClient.deletePipelineById(pipelineId)
          Thread.sleep(15000)  // wait until the data pipeline is really deleted
          createPipeline(false)
        } else {
          println("User --force to force pipeline creation")
          None
        }

      case None =>
        val pipelineId = client.createPipeline(
            new CreatePipelineRequest()
              .withUniqueId(pipelineName)
              .withName(pipelineName)
          ).getPipelineId
        println(s"Pipeline created: $pipelineId")
        println("Uploading pipeline definition")
        val pipelineObjects: Seq[PipelineObject] = pipelineDef
        val paramObjects: Seq[ParameterObject] = pipelineDef
        val putDefinitionResult = client.putPipelineDefinition(
          new PutPipelineDefinitionRequest()
            .withPipelineId(pipelineId)
            .withPipelineObjects(pipelineObjects)
            .withParameterObjects(paramObjects)
        )
        putDefinitionResult.getValidationErrors.flatMap(_.getErrors().map(e => s"ERROR: $e"))
          .foreach(println)
        putDefinitionResult.getValidationWarnings.flatMap(_.getWarnings().map(e => s"WARNING: $e"))
          .foreach(println)
        if (putDefinitionResult.getErrored) {
          println("Failed to create pipeline")
          println("Deleting the just created pipeline")
          HyperionAwsClient.deletePipelineById(pipelineId)
          None
        } else if (putDefinitionResult.getValidationErrors.isEmpty()
              && putDefinitionResult.getValidationWarnings.isEmpty()) {
            println("Successfully created pipeline")
            Some(pipelineId)
        } else {
            println("Successful with warnings")
            Some(pipelineId)
        }
    }
  }

  private def pipelineNameAction(action: (String) => Unit): Unit =
    getPipelineId match {
      case Some(pipelineId) => action(pipelineId)
      case None => println(s"Pipeline $pipelineName does not exist")
    }

  def activatePipeline(): Unit = pipelineNameAction(HyperionAwsClient.activatePipelineById)

  def deletePipeline(): Unit = pipelineNameAction(HyperionAwsClient.deletePipelineById)

}

object HyperionAwsClient {

  lazy val client = new DataPipelineClient

  def deletePipelineById(pipelineId: String): Unit = {
    println(s"Deleting pipeline $pipelineId")
    client.deletePipeline(new DeletePipelineRequest().withPipelineId(pipelineId))
  }

  def activatePipelineById(pipelineId: String): Unit = {
    println(s"Activating pipeline $pipelineId")
    client.activatePipeline(new ActivatePipelineRequest().withPipelineId(pipelineId))
  }

}
