package com.krux.hyperion.cli

import java.io.PrintStream

import com.krux.hyperion.DataPipelineDef
import com.krux.hyperion.workflow.WorkflowGraphRenderer

case object GraphAction extends Action {
  def execute(options: Options, pipelineDef: DataPipelineDef): Int = {
    val renderer = WorkflowGraphRenderer(pipelineDef, options.removeLastNameSegment,
      options.label, options.includeResources, options.includeDataNodes, options.includeDatabases)
    options.output.map(f => new PrintStream(f)).getOrElse(System.out).println(renderer.render())
    0
  }
}
