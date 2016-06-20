package com.krux.hyperion.cli

import java.io.PrintStream

import com.krux.hyperion.DataPipelineDefGroup
import com.krux.hyperion.workflow.WorkflowGraphRenderer

private[hyperion] case object GraphAction extends Action {

  def apply(options: Options, defGroup: DataPipelineDefGroup): Boolean = {

    defGroup.ungroup().foreach { case (key, pipelineDef) =>
      val renderer = WorkflowGraphRenderer(pipelineDef, options.removeLastNameSegment,
        options.label, options.includeResources, options.includeDataNodes, options.includeDatabases)
      options.output
        .map(f => new PrintStream(f + key.map("#" + _).getOrElse("") + ".dot"))
        .getOrElse(System.out)
        .println(renderer.render())
    }
    true

  }

}
