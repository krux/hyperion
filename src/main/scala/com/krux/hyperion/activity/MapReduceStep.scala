package com.krux.hyperion.activity

/**
 * A MapReduce step that runs on MapReduce Cluster
 */
case class MapReduceStep(
  jar: Option[String] = None,
  mainClass: Option[String] = None,
  args: Seq[String] = Seq()
) {

  def withJar(jar: String) = this.copy(jar = Option(jar))
  def withMainClass(mainClass: String) = this.copy(mainClass = Option(mainClass.stripSuffix("$")))
  def withArguments(arg: String*) = this.copy(args = args ++ arg)

  def toStepString = (jar.toSeq ++ mainClass.toSeq ++ args).mkString(",")

}
