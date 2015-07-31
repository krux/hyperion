package com.krux.hyperion.activity

/**
 * A MapReduce step that runs on MapReduce Cluster
 */
case class MapReduceStep private (
  jarUri: String,
  mainClass: Option[String],
  args: Seq[String]
) {

  def withMainClass(mainClass: Any) = this.copy(mainClass = ActivityHelper.getMainClass(mainClass))
  def withArguments(arg: String*) = this.copy(args = args ++ arg)

  override def toString: String = (Seq(jarUri) ++ mainClass.toSeq ++ args).mkString(",")

}

object MapReduceStep {

  def apply(jarUri: String): MapReduceStep = MapReduceStep(
    jarUri = jarUri,
    mainClass = None,
    args = Seq()
  )

}

