package com.krux.hyperion.resource

trait EmrApplication {
  val name: String
  lazy val serialize: String = name
}

object EmrApplication {

  case object Spark extends EmrApplication {
    val name = "Spark"
  }

}
