package com.krux.hyperion.cli

import com.krux.hyperion.io.AwsClientForDef

private[hyperion] case object DeleteAction extends AwsAction {

  def apply(options: Options, client: AwsClientForDef): Boolean =
    client.forName().flatMap(_.forId()).flatMap(_.deletePipelines()).isDefined

}
