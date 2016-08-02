package com.krux.hyperion.cli

import com.krux.hyperion.client.AwsClientForDef

private[hyperion] case object ActivateAction extends AwsAction {
  def apply(options: Options, client: AwsClientForDef): Boolean =
    client.forName().flatMap(_.forId()).flatMap(_.activatePipelines()).isDefined
}
