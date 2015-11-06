package com.krux.hyperion.cli

import com.krux.hyperion.HyperionAwsPipelineClient

case object ActivateAction extends AwsAction {
  def execute(options: Options, client: HyperionAwsPipelineClient): Boolean = client.activatePipeline()
}
