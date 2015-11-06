package com.krux.hyperion.cli

import com.krux.hyperion.DataPipelineDef

case object UnknownAction extends Action {
  def execute(options: Options, pipelineDef: DataPipelineDef): Int = -1
}
