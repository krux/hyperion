package com.krux.hyperion.cli

import com.krux.hyperion.HyperionAwsClient

private[hyperion] case object CreateAction extends AwsAction {

  def apply(options: Options, client: HyperionAwsClient): Boolean =
    client.createPipeline(options.force, options.activate)

}
