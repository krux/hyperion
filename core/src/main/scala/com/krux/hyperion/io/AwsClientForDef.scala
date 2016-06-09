package com.krux.hyperion.io

import scala.collection.JavaConverters._

import com.amazonaws.services.datapipeline.DataPipelineClient
import com.amazonaws.services.datapipeline.model.{ PipelineObject, ListPipelinesRequest,
  ParameterObject, CreatePipelineRequest, Tag, PutPipelineDefinitionRequest }

import com.krux.hyperion.DataPipelineDefGroup


case class AwsClientForDef(
    client: DataPipelineClient, pipelineDef: DataPipelineDefGroup
  ) extends AwsClient {

  def createPipelines(force: Boolean): Option[AwsClientForId] = {
    log.info(s"Creating pipline ${pipelineDef.pipelineName}")
    prepareForCreation(force).flatMap(_.uploadPipelineObjects())
  }

  def forName(): Option[AwsClientForName] = Option(
    AwsClientForName(client, pipelineDef.pipelineName, pipelineDef.nameKeySeparator)
  )

  /**
   * Check and prepare for the creation of pipleines
   *
   * @return Some of the this AwsClientForDef if there are no existing pipelines (or use force
   * after the exisitng pipeline has been deleted), and None if existing piplines exist and force
   * is not used.
   */
  private def prepareForCreation(force: Boolean): Option[AwsClientForDef] = {

    val pipelineMasterName = pipelineDef.pipelineName
    val pipelineNames = pipelineDef.workflows.keys

    val existingPipelines =
      AwsClientForName(client, pipelineMasterName, pipelineDef.nameKeySeparator).pipelineIdNames

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
  private def uploadPipelineObjects(): Option[AwsClientForId] =
    UploadPipelineObjectsTrans(client, pipelineDef)().right.toOption

}
