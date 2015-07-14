package com.krux.hyperion.activity

/**
 * A MapReduce step that runs on MapReduce Cluster
 */
case class MapReduceStep private (
  jar: Option[String],
  mainClass: Option[String],
  args: Seq[String]
) {

  def withJar(jar: String) = this.copy(jar = Option(jar))
  def withMainClass(mainClass: Any): MapReduceStep = mainClass match {
    case mc: String => this.copy(mainClass = Option(mc.stripSuffix("$")))
    case mc: Class[_] => this.withMainClass(mc.getName)
    case mc => this.withMainClass(mc.getClass)
  }
  def withArguments(arg: String*) = this.copy(args = args ++ arg)

  override def toString: String = (jar.toSeq ++ mainClass.toSeq ++ args).mkString(",")

}

object MapReduceStep {

  def apply(): MapReduceStep = MapReduceStep(
    jar = None,
    mainClass = None,
    args = Seq()
  )

}

