package com.krux.hyperion.client

import com.amazonaws.services.datapipeline.DataPipelineClient
import com.amazonaws.services.datapipeline.model.{ DeactivatePipelineRequest,
  DeletePipelineRequest, ActivatePipelineRequest }


case class AwsClientForId(
  client: DataPipelineClient,
  pipelineIds: Set[String]
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
      log.info(s"Activating pipeline $id")

      client
        .deactivatePipeline(
          new DeactivatePipelineRequest().withPipelineId(id).withCancelActive(true)
        )
        .retry()
    }
    Option(this)
  }

}
