package com.krux.hyperion.activity

import com.krux.hyperion.adt.{HString, HS3Uri}


case class EmrStep private (
  jarUri: HString,
  args: Seq[HString]
) extends BaseEmrStep {

  def mainClass = None
  def withArguments(newArgs: HString*) = copy(args = args ++ newArgs)

}

object EmrStep {

  def apply(jarUri: HS3Uri): EmrStep = apply(jarUri.serialize)

  def apply(jarUri: HString): EmrStep = new EmrStep(jarUri, Seq.empty)

  /**
   * https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-commandrunner.html
   */
  def commandRunner(command: String) = apply(EmrCommandRunner).withArguments(command)

  def scriptRunner(script: HS3Uri) = apply(EmrScriptRunner).withArguments(script.toString)

}
