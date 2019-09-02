package com.krux.hyperion.client

import com.amazonaws.services.datapipeline.DataPipeline
import com.amazonaws.services.datapipeline.model.{DeactivatePipelineRequest,
  DeletePipelineRequest, ActivatePipelineRequest}


case class AwsClientForId(
  client: DataPipeline,
  region: String,
  pipelineIds: Set[String],
  override val maxRetry: Int
) extends AwsClient {

  def deletePipelines(): Option[Unit] = {
    pipelineIds.foreach { id =>
      log.info(s"Deleting pipeline $id")
      client.deletePipeline(new DeletePipelineRequest().withPipelineId(id)).retry()
    }
    Option(Unit)
  }

  def activatePipelines(): Option[AwsClientForId] = {
    pipelineIds.foreach { id =>
      log.info(s"Activating pipeline $id")
      client.activatePipeline(new ActivatePipelineRequest().withPipelineId(id)).retry()
    }
    Option(this)
  }

  def deactivatePipelines(): Option[AwsClientForId] = {
    pipelineIds.foreach { id =>
      log.info(s"Deactivating pipeline $id")

      client
        .deactivatePipeline(
          new DeactivatePipelineRequest().withPipelineId(id).withCancelActive(true)
        )
        .retry()
    }
    Option(this)
  }

  def printConsoleUrl(): Option[Unit] = {
    pipelineIds.foreach { id =>
      log.info(s"https://console.aws.amazon.com/datapipeline/home?region=$region#ExecutionDetailsPlace:pipelineId=$id&show=latest")
    }
    Option(Unit)
  }

}
