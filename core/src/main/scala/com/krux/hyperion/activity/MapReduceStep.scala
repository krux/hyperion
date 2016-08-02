package com.krux.hyperion.activity

import com.krux.hyperion.adt.{HString, HS3Uri}
import com.krux.hyperion.common.Escapable

/**
 * A MapReduce step that runs on MapReduce Cluster
 */
case class MapReduceStep private (
  jarUri: HString,
  mainClass: Option[MainClass],
  args: Seq[HString]
) {

  def withMainClass(mainClass: MainClass) = copy(mainClass = Option(mainClass))
  def withArguments(arg: HString*) = copy(args = args ++ arg)

  def serialize: String = (jarUri +: mainClass.map(_.toString).toSeq ++: args)
    .map(x => Escapable.escape(x.toString, ','))
    .mkString(",")

  override def toString = serialize

}

object MapReduceStep {

  def apply(jarUri: HS3Uri): MapReduceStep = apply(jarUri.serialize)

  def apply(jarUri: HString): MapReduceStep = MapReduceStep(
    jarUri = jarUri,
    mainClass = None,
    args = Seq.empty
  )

}

