package com.krux.hyperion

import com.amazonaws.services.datapipeline.DataPipelineClient
import com.amazonaws.services.datapipeline.model.PipelineObject


case class PipelineGroupAwsClient(
  awsClient: DataPipelineClient,
  pipelineDef: AbstractDataPipelineDef
) {

  lazy val pipelineIdNames: Seq[String] = ???

  lazy val pipelineNames: Set[String] = pipelineObjects.keySet.map { key =>
    pipelineDef.pipelineName + key.map("#" + _).getOrElse("")
  }

  lazy val pipelineObjects: Map[WorkflowKey, Seq[PipelineObject]] =
    pipelineDef.toAwsPipelineObjects

}

object PipelineGroupAwsClient {
}
