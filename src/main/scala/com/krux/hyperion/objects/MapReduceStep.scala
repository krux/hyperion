package com.krux.hyperion.objects

/**
 * A MapReduce step that runs on MapReduce Cluster
 */
case class MapReduceStep(
  jar: String = "",
  mainClass: String = "",
  args: Seq[String] = Seq()
) {

  def withJar(jar: String) = this.copy(jar = jar)
  def withMainClass(mainClass: String) = this.copy(mainClass = mainClass)
  def withArgs(arg: String*) = this.copy(args = args ++ arg)
  def withArgSeq(args: Seq[String]) = this.copy(args = args)

  def toStepString = (jar +: mainClass +: args).mkString(",")

}
