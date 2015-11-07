package com.krux.hyperion.cli

import java.io.PrintStream

import com.krux.hyperion.DataPipelineDef
import org.json4s.jackson.JsonMethods._

private[hyperion] case object GenerateAction extends Action {
  def execute(options: Options, pipelineDef: DataPipelineDef): Boolean = {
    options.output.map(f => new PrintStream(f)).getOrElse(System.out).println(pretty(render(pipelineDef)))
    true
  }
}
