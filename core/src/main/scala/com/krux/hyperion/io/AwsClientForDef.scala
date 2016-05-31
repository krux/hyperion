package com.krux.hyperion.io

import scala.collection.JavaConverters._

import com.amazonaws.services.datapipeline.DataPipelineClient
import com.amazonaws.services.datapipeline.model.{ PipelineObject, ListPipelinesRequest,
  ParameterObject, CreatePipelineRequest, Tag, PutPipelineDefinitionRequest }

import com.krux.hyperion.AbstractDataPipelineDef


case class AwsClientForDef(
    client: DataPipelineClient, pipelineDef: AbstractDataPipelineDef
  ) extends AwsClient {

  def createPipelines(force: Boolean): Option[AwsClientForId] = {
    log.info(s"Creating pipline ${pipelineDef.pipelineName}")
    prepareForCreation(force).flatMap(_.uploadPipelineObjects())
  }

  /**
   * Check and prepare for the creation of pipleines
   */
  private def prepareForCreation(force: Boolean): Option[AwsClientForDef] = {

    val pipelineMasterName = pipelineDef.pipelineName
    val pipelineNames = pipelineDef.pipelineNames

    val existingPipelines =
      AwsClientForName(client, pipelineMasterName, pipelineDef.NameKeySeparator).pipelineIdNames

    if (existingPipelines.values.toSet != pipelineNames) log.warn("inconsistent data pipline names")

    if (existingPipelines.nonEmpty) {
      log.warn("Pipeline group already exists")
      if (force) {
        log.info("Delete the exisiting pipline")
        AwsClientForId(client, existingPipelines.keySet).deletePipelines()
        prepareForCreation(force)
      } else {
        log.error("Use --force to force pipeline creation")
        None
      }
    } else {
      Some(this)
    }

  }

  /**
   * Create and upload the pipeline definitions, if error occurs a full roll back is issued.
   */
  private def uploadPipelineObjects(): Option[AwsClientForId] = {

    val parameterObjects = pipelineDef.toAwsParameters

    def createAndUploadObjects(name: String, objects: Seq[PipelineObject]): Option[String] = {

      val pipelineId = throttleRetry(
        client.createPipeline(
          new CreatePipelineRequest()
            .withUniqueId(name)
            .withName(name)
            .withTags(
              pipelineDef.tags.toSeq
                .map { case (k, v) => new Tag().withKey(k).withValue(v.getOrElse("")) }
                .asJava
            )
        )
        .getPipelineId
      )

      log.info(s"Created pipeline $pipelineId ($name)")
      log.info(s"Uploading pipline definition to $pipelineId")

      val putDefinitionResult = throttleRetry(
        client.putPipelineDefinition(
          new PutPipelineDefinitionRequest()
            .withPipelineId(pipelineId)
            .withPipelineObjects(objects.asJava)
            .withParameterObjects(parameterObjects.asJava)
        )
      )

      putDefinitionResult.getValidationErrors.asScala
        .flatMap(err => err.getErrors.asScala.map(detail => s"${err.getId}: $detail"))
        .foreach(log.error)
      putDefinitionResult.getValidationWarnings.asScala
        .flatMap(err => err.getWarnings.asScala.map(detail => s"${err.getId}: $detail"))
        .foreach(log.warn)

      if (putDefinitionResult.getErrored) {
        log.error("Failed to create pipeline")
        log.error("Deleting the just created pipeline")
        AwsClientForId(client, Set(pipelineId)).deletePipelines()
        None
      } else if (putDefinitionResult.getValidationErrors.isEmpty
        && putDefinitionResult.getValidationWarnings.isEmpty) {
        log.info("Successfully created pipeline")
        Option(pipelineId)
      } else {
        log.warn("Successful with warnings")
        Option(pipelineId)
      }
    }

    val keyObjectsMap = pipelineDef.toAwsPipelineObjects

    val afterUpload = keyObjectsMap.flatMap { case (key, objects) =>
      createAndUploadObjects(pipelineDef.pipelineNameForKey(key), objects)
    }

    val retClient = AwsClientForId(client, afterUpload.toSet)
    if (afterUpload.size != keyObjectsMap.size) {
      retClient.deletePipelines()
      None
    } else {
      Option(retClient)
    }

  }

}
