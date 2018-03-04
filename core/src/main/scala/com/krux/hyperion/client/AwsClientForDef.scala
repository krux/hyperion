package com.krux.hyperion.client

import com.amazonaws.services.datapipeline.DataPipeline

import com.krux.hyperion.DataPipelineDefGroup


case class AwsClientForDef(
  client: DataPipeline,
  pipelineDef: DataPipelineDefGroup
) extends AwsClient {

  override lazy val maxRetry = pipelineDef.hc.maxRetry

  def createPipelines(force: Boolean, checkExistence: Boolean): Option[AwsClientForId] = {
    log.info(s"Creating pipeline ${pipelineDef.pipelineName}")
    prepareForCreation(force, checkExistence).flatMap(_.uploadPipelineObjects())
  }

  def forName(): Option[AwsClientForName] = Option(
    AwsClientForName(client, pipelineDef.pipelineName, maxRetry, pipelineDef.nameKeySeparator)
  )

  /**
   * Check and prepare for the creation of pipelines
   *
   * @return Some of the this AwsClientForDef if there are no existing pipelines (or use force
   * after the existing pipeline has been deleted), and None if existing pipelines exist and force
   * is not used.
   */
  private def prepareForCreation(force: Boolean, checkExistence: Boolean): Option[AwsClientForDef] = {

    lazy val pipelineNames = {
      val result = pipelineDef.workflows.keys.map(pipelineDef.nameForKey)
      assert(result.nonEmpty, "Pipeline must define at least one workflow")
      result
    }

    val existingPipelines =
      if (checkExistence)
        AwsClientForName(client, pipelineDef.pipelineName, maxRetry, pipelineDef.nameKeySeparator)
          .pipelineIdNames
      else
        Map.empty[String, String]

    if (existingPipelines.nonEmpty) {
      log.warn("Pipeline group already exists")

      if (existingPipelines.values.toSet != pipelineNames)
        log.warn(s"Inconsistent data pipeline names: AWS had (${existingPipelines.values.toSet.mkString(", ")}), the pipeline defined (${pipelineNames.mkString(", ")})")

      if (force) {
        log.info("Delete the existing pipeline")
        AwsClientForId(client, existingPipelines.keySet, maxRetry).deletePipelines()
        prepareForCreation(force, checkExistence)
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
    UploadPipelineObjectsTrans(client, pipelineDef, maxRetry)().right.toOption

}
