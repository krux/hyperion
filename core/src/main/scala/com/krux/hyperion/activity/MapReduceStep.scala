package com.krux.hyperion.activity

import com.krux.hyperion.adt.HString

/**
 * A MapReduce step that runs on MapReduce Cluster
 */
case class MapReduceStep private (
  jarUri: HString,
  mainClass: Option[MainClass],
  args: Seq[HString]
) {

  def withMainClass(mainClass: MainClass) = this.copy(mainClass = Option(mainClass))
  def withArguments(arg: HString*) = this.copy(args = args ++ arg)

  override def toString = (Seq(jarUri) ++ mainClass.map(_.toString).toSeq ++ args).mkString(",")

}

object MapReduceStep {

  def apply(jarUri: HString): MapReduceStep = MapReduceStep(
    jarUri = jarUri,
    mainClass = None,
    args = Seq.empty
  )

}

