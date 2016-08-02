package com.krux.hyperion.cli

import com.krux.hyperion.client.{AwsClientForDef, AwsClient}
import com.krux.hyperion.DataPipelineDefGroup

private[hyperion] trait AwsAction extends Action {

  def apply(options: Options, client: AwsClientForDef): Boolean

  def apply(options: Options, pipelineDef: DataPipelineDefGroup): Boolean =
    apply(options, AwsClient(pipelineDef, options.region, options.roleArn))

}
