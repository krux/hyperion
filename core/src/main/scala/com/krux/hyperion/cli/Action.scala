package com.krux.hyperion.cli

import com.krux.hyperion.DataPipelineDefGroup

private[hyperion] trait Action {
  def apply(options: Options, pipelineDef: DataPipelineDefGroup): Boolean
}
