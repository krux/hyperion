package com.krux.hyperion.cli

import com.krux.hyperion.{HyperionAwsClient, DataPipelineDef}

case object CreateAction extends Action {
  def execute(options: Options, pipelineDef: DataPipelineDef): Int = {
    val awsClient = new HyperionAwsClient(options.region, options.roleArn)
    val awsClientForPipeline = awsClient.ForPipelineDef(pipelineDef)

    awsClientForPipeline.createPipeline(options.force) match {
      case Some(id) if options.activate =>
        if (awsClient.ForPipelineId(id).activatePipelineById()) 0 else 3

      case None =>
        3

      case _ =>
        0
    }
  }
}
