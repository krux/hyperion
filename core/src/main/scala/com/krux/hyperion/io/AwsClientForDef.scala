package com.krux.hyperion.io

import scala.annotation.tailrec
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
    val existingPipelines = getPipelineIdNames()

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

  private def getPipelineIdNames(): Map[String, String] = {

    val pipelineMasterName = pipelineDef.pipelineName

    def inGroup(actualName: String): Boolean = (
      actualName == pipelineMasterName ||
      actualName.startsWith(pipelineMasterName + pipelineDef.NameKeySeparator)
    )

    @tailrec
    def queryPipelines(
        idNames: Map[String, String] = Map.empty,
        request: ListPipelinesRequest = new ListPipelinesRequest()
      ): Map[String, String] = {

      val response = throttleRetry(client.listPipelines(request))
      val theseIdNames = response.getPipelineIdList
        .asScala
        .collect { case idName if inGroup(idName.getName) => (idName.getId, idName.getName) }
        .toMap

      if (response.getHasMoreResults)
        queryPipelines(
          idNames ++ theseIdNames,
          new ListPipelinesRequest().withMarker(response.getMarker)
        )
      else
        idNames ++ theseIdNames

    }

    def checkResults(idNames: Map[String, String]): Unit = {
      val names = idNames.values.toSet

      // if using Hyperion for all DataPipeline management, this should never happen
      if (names.size != idNames.size) throw new RuntimeException("Duplicated pipeline name")

      if (names != pipelineDef.pipelineNames) log.warn("inconsistent data pipline names")

      if (idNames.isEmpty) log.debug(s"Pipeline ${pipelineMasterName} does not exist")

    }

    val result = queryPipelines()
    checkResults(result)
    result

  }

}
