package com.krux.hyperion.cli

import com.krux.hyperion.HyperionAwsPipelineClient

case object CreateAction extends AwsAction {

  def execute(options: Options, client: HyperionAwsPipelineClient): Boolean =
    client.createPipeline(options.force, options.activate)

}
