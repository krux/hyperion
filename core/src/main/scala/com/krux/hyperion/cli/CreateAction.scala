package com.krux.hyperion.cli

import com.krux.hyperion.client.AwsClientForDef

private[hyperion] case object CreateAction extends AwsAction {

  def apply(options: Options, client: AwsClientForDef): Boolean = {
    if (options.activate)
      client.createPipelines(options.force, options.checkExistence).flatMap(_.activatePipelines())
    else
      client.createPipelines(options.force, options.checkExistence)
  }.isDefined

}
