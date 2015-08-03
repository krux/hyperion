package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.common.S3Uri
import com.krux.hyperion.parameter.Parameter

/**
 * A spark step that runs on Spark Cluster
 */
case class SparkStep private (
  jarUri: Parameter[S3Uri],
  mainClass: Option[MainClass],
  args: Seq[String],
  scriptRunner: Option[String],
  jobRunner: Option[String]
) {

  def withMainClass(mainClass: MainClass) = this.copy(mainClass = Option(mainClass))
  def withArguments(arg: String*) = this.copy(args = args ++ arg)

  override def toString: String = (scriptRunner.toSeq ++ jobRunner.toSeq ++ Seq(jarUri.toString) ++ mainClass.map(_.toString).toSeq ++ args).mkString(",")

}

object SparkStep {

  def apply(jarUri: Parameter[S3Uri])(implicit hc: HyperionContext): SparkStep = SparkStep(
    jarUri = jarUri,
    mainClass = None,
    args = Seq(),
    scriptRunner = Option("s3://elasticmapreduce/libs/script-runner/script-runner.jar"),
    jobRunner = Option(s"${hc.scriptUri}run-spark-step.sh")
  )

}
