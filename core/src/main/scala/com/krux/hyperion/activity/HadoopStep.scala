package com.krux.hyperion.activity

import com.krux.hyperion.adt.{HString, HS3Uri}


/**
 * Same as EmrStep but can specify mainClass
 */
case class HadoopStep private (
  jarUri: HString,
  mainClass: Option[MainClass],
  args: Seq[HString]
) extends BaseEmrStep {

  def withMainClass(mainClass: MainClass) = copy(mainClass = Option(mainClass))
  def withArguments(newArgs: HString*) = copy(args = args ++ newArgs)

}

object HadoopStep {

  def apply(jarUri: HS3Uri): HadoopStep = apply(jarUri.serialize)

  def apply(jarUri: HString): HadoopStep = new HadoopStep(jarUri, None, Seq.empty)

}
