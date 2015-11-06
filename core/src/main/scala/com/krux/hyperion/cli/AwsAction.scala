package com.krux.hyperion.cli

import com.krux.hyperion.{HyperionAwsPipelineClient, HyperionAwsClient, DataPipelineDef}

trait AwsAction extends Action {

  def execute(options: Options, client: HyperionAwsPipelineClient): Boolean

  def execute(options: Options, pipelineDef: DataPipelineDef): Boolean =
    execute(options, new HyperionAwsClient(options.region, options.roleArn).ForPipelineDef(pipelineDef))

}
