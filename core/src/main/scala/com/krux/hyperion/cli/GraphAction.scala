package com.krux.hyperion.cli

import java.io.PrintStream

import com.krux.hyperion.DataPipelineDef
import com.krux.hyperion.workflow.WorkflowGraphRenderer

private[hyperion] case object GraphAction extends Action {
  def apply(options: Options, pipelineDef: DataPipelineDef): Boolean = {
    val renderer = WorkflowGraphRenderer(pipelineDef, options.removeLastNameSegment,
      options.label, options.includeResources, options.includeDataNodes, options.includeDatabases)
    options.output.map(f => new PrintStream(f)).getOrElse(System.out).println(renderer.render())
    true
  }
}
