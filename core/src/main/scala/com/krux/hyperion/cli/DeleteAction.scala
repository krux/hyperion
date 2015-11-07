package com.krux.hyperion.cli

import com.krux.hyperion.HyperionAwsClient

private[hyperion] case object DeleteAction extends AwsAction {
  def execute(options: Options, client: HyperionAwsClient): Boolean = client.deletePipeline()
}
