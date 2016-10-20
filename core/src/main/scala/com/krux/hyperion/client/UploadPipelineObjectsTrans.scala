package com.krux.hyperion.client

import scala.collection.JavaConverters._

import com.amazonaws.services.datapipeline.DataPipelineClient
import com.amazonaws.services.datapipeline.model.{
  PipelineObject, ListPipelinesRequest, ParameterObject, CreatePipelineRequest, Tag,
  PutPipelineDefinitionRequest, InvalidRequestException
}
import org.slf4j.LoggerFactory

import com.krux.hyperion.DataPipelineDefGroup


case class UploadPipelineObjectsTrans(
  client: DataPipelineClient,
  pipelineDef: DataPipelineDefGroup,
  maxRetry: Int
) extends Transaction[Option[Unit], AwsClientForId] with Retry {

  val log = LoggerFactory.getLogger(getClass)

  val parameterObjects = pipelineDef.toAwsParameters

  val keyObjectsMap = pipelineDef.toAwsPipelineObjects

  private def createAndUploadObjects(name: String, objects: Seq[PipelineObject]): Option[String] = {

    val pipelineId = client
      .createPipeline(
        new CreatePipelineRequest()
          .withUniqueId(name)
          .withName(name)
          .withTags(
            pipelineDef.tags.toSeq
              .map { case (k, v) => new Tag().withKey(k).withValue(v.getOrElse("")) }
              .asJava
          )
      )
      .retry()
      .getPipelineId

    log.info(s"Created pipeline $pipelineId ($name)")
    log.info(s"Uploading pipeline definition to $pipelineId")

    try {

      val putDefinitionResult = client
        .putPipelineDefinition(
          new PutPipelineDefinitionRequest()
            .withPipelineId(pipelineId)
            .withPipelineObjects(objects.asJava)
            .withParameterObjects(parameterObjects.asJava)
        )
        .retry()

      putDefinitionResult.getValidationErrors.asScala
        .flatMap(err => err.getErrors.asScala.map(detail => s"${err.getId}: $detail"))
        .foreach(log.error)
      putDefinitionResult.getValidationWarnings.asScala
        .flatMap(err => err.getWarnings.asScala.map(detail => s"${err.getId}: $detail"))
        .foreach(log.warn)

      if (putDefinitionResult.getErrored) {
        log.error(s"Failed to upload pipeline definition to pipeline $pipelineId")
        log.error(s"Deleting the just created pipeline $pipelineId")
        AwsClientForId(client, Set(pipelineId), maxRetry).deletePipelines()
        None
      } else if (putDefinitionResult.getValidationErrors.isEmpty
        && putDefinitionResult.getValidationWarnings.isEmpty) {
        log.info("Successfully created pipeline")
        Option(pipelineId)
      } else {
        log.warn("Successful with warnings")
        Option(pipelineId)
      }

    } catch {
      case e: InvalidRequestException =>
        log.error(s"InvalidRequestException (${e.getErrorCode}): ${e.getErrorMessage}")
        log.error("Deleting the just created pipeline")
        AwsClientForId(client, Set(pipelineId), maxRetry).deletePipelines()
        None
    }

  }

  def action() = AwsClientForId(
    client,
    keyObjectsMap
      .toStream  // there is no need to keep perform createAndUploadObojects if one failed
      .map { case (key, objects) =>
        log.info(s"Creating pipeline and uploading ${objects.size} objects")
        createAndUploadObjects(pipelineDef.nameForKey(key), objects)
      }
      .takeWhile(_.nonEmpty)
      .flatten
      .toSet,
    maxRetry
  )

  def validate(result: AwsClientForId) = result.pipelineIds.size == keyObjectsMap.size

  def rollback(result: AwsClientForId) = result.deletePipelines()

}
