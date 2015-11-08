package com.krux.hyperion.cli

import com.krux.hyperion.HyperionAwsClient

private[hyperion] case object ActivateAction extends AwsAction {
  def apply(options: Options, client: HyperionAwsClient): Boolean = client.activatePipeline()
}
