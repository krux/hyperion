package com.krux.hyperion.cli

import com.krux.hyperion.DataPipelineDef

private[hyperion] trait Action {
  def apply(options: Options, pipelineDef: DataPipelineDef): Boolean
}
