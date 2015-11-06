package com.krux.hyperion.cli

import com.krux.hyperion.{HyperionAwsClient, DataPipelineDef}

case object DeleteAction extends Action {
  def execute(options: Options, pipelineDef: DataPipelineDef): Int = {
    val awsClient = new HyperionAwsClient(options.region, options.roleArn)
    val awsClientForPipeline = awsClient.ForPipelineDef(pipelineDef)

    if (awsClientForPipeline.deletePipeline()) 0 else 3
  }
}
