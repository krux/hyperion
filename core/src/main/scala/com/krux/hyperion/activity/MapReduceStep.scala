package com.krux.hyperion.activity

/**
 * A MapReduce step that runs on MapReduce Cluster
 */
case class MapReduceStep private (
  jarUri: String,
  mainClass: Option[MainClass],
  args: Seq[String]
) {

  def withMainClass(mainClass: MainClass) = this.copy(mainClass = Option(mainClass))
  def withArguments(arg: String*) = this.copy(args = args ++ arg)

  override def toString: String = (Seq(jarUri) ++ mainClass.map(_.toString).toSeq ++ args).mkString(",")

}

object MapReduceStep {

  def apply(jarUri: String): MapReduceStep = MapReduceStep(
    jarUri = jarUri,
    mainClass = None,
    args = Seq()
  )

}

