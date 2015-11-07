package com.krux.hyperion.cli

import com.krux.hyperion.{HyperionAwsClient, DataPipelineDef}

private[hyperion] trait AwsAction extends Action {

  def execute(options: Options, client: HyperionAwsClient): Boolean

  def execute(options: Options, pipelineDef: DataPipelineDef): Boolean =
    execute(options, HyperionAwsClient(pipelineDef, options.region, options.roleArn))

}
