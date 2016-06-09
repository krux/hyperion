package com.krux.hyperion

import com.krux.hyperion.cli.EntryPoint

/**
  * HyperionCli is a base trait that brings in CLI functionality to
  * DataPipelineDef's.
  */
trait HyperionCli { this: DataPipelineDefGroup =>

  def main(args: Array[String]): Unit = System.exit(EntryPoint(this).run(args))

}

