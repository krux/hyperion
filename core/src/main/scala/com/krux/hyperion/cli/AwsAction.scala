package com.krux.hyperion.cli

import com.krux.hyperion.DataPipelineDef
import com.krux.hyperion.io.{ AwsClientForDef, AwsClient }

private[hyperion] trait AwsAction extends Action {

  def apply(options: Options, client: AwsClientForDef): Boolean

  def apply(options: Options, pipelineDef: DataPipelineDef): Boolean =
    apply(options, AwsClient(pipelineDef, options.region, options.roleArn))

}
