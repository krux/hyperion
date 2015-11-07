package com.krux.hyperion

import com.krux.hyperion.cli._

/**
  * HyperionCli is a base trait that brings in CLI functionality to
  * DataPipelineDef's.
  */
trait HyperionCli { this: DataPipelineDef =>

  def main(args: Array[String]): Unit = System.exit(EntryPoint(this).run(args))

}

