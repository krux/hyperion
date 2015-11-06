package com.krux.hyperion.cli

import com.krux.hyperion.DataPipelineDef

trait Action {
  def execute(options: Options, pipelineDef: DataPipelineDef): Int
}
