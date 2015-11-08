package com.krux.hyperion.cli

import com.krux.hyperion.{HyperionAwsClient, DataPipelineDef}

private[hyperion] trait AwsAction extends Action {

  def apply(options: Options, client: HyperionAwsClient): Boolean

  def apply(options: Options, pipelineDef: DataPipelineDef): Boolean =
    apply(options, HyperionAwsClient(pipelineDef, options.region, options.roleArn))

}
