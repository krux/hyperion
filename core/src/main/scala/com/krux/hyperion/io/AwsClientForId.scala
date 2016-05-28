package com.krux.hyperion.io

import com.amazonaws.services.datapipeline.DataPipelineClient
import com.amazonaws.services.datapipeline.model.{ DeletePipelineRequest, ActivatePipelineRequest }


case class AwsClientForId(
    client: DataPipelineClient,
    pipelineIds: Set[String]
  ) extends AwsClient {

  def deletePipelines(): Unit = pipelineIds.foreach { id =>
    log.info(s"Deleting pipeline $id")
    throttleRetry(client.deletePipeline(new DeletePipelineRequest().withPipelineId(id)))
  }

  def activatePipelines(): Unit = pipelineIds.foreach { id =>
    log.info(s"Activating pipeline $id")
    throttleRetry(client.activatePipeline(new ActivatePipelineRequest().withPipelineId(id)))
  }

}
