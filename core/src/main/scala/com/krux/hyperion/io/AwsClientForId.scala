package com.krux.hyperion.io

import com.amazonaws.services.datapipeline.DataPipelineClient

case class AwsClientForId(
    client: DataPipelineClient,
    pipelineIds: Set[String]
  ) extends AwsClient {


  def deletePipelines(): Boolean = ???

}
