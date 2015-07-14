package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext

/**
 * A spark step that runs on Spark Cluster
 */
case class SparkStep(
  jar: Option[String] = None,
  mainClass: Option[String] = None,
  args: Seq[String] = Seq()
)(implicit hc: HyperionContext) {

  val scriptRunner = Option("s3://elasticmapreduce/libs/script-runner/script-runner.jar")
  val jobRunner = Option(s"${hc.scriptUri}run-spark-step.sh")

  def withJar(jar: String) = this.copy(jar = Option(jar))
  def withMainClass(mainClass: String) = this.copy(mainClass = Option(mainClass.stripSuffix("$")))
  def withArguments(arg: String*) = this.copy(args = args ++ arg)

  def toStepString = (scriptRunner.toSeq ++ jobRunner.toSeq ++ jar.toSeq ++ mainClass.toSeq ++ args).mkString(",")

}
