package com.krux.hyperion.activity

/**
 * A MapReduce step that runs on MapReduce Cluster
 */
case class MapReduceStep private (
  jar: String,
  mainClass: Option[String],
  args: Seq[String]
) {

  def withMainClass(mainClass: Any) = this.copy(mainClass = ActivityHelper.getMainClass(mainClass))
  def withArguments(arg: String*) = this.copy(args = args ++ arg)

  override def toString: String = (Seq(jar) ++ mainClass.toSeq ++ args).mkString(",")

}

object MapReduceStep {

  def apply(jar: String): MapReduceStep = MapReduceStep(
    jar = jar,
    mainClass = None,
    args = Seq()
  )

}

