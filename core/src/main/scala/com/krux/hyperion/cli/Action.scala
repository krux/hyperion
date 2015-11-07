package com.krux.hyperion.cli

import com.krux.hyperion.DataPipelineDef

private[hyperion] trait Action {
  def execute(options: Options, pipelineDef: DataPipelineDef): Boolean
}
