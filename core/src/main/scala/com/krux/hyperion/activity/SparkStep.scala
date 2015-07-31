package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext

/**
 * A spark step that runs on Spark Cluster
 */
case class SparkStep private (
  jar: String,
  mainClass: Option[String],
  args: Seq[String],
  scriptRunner: Option[String],
  jobRunner: Option[String]
) {

  def withMainClass(mainClass: Any) = this.copy(mainClass = ActivityHelper.getMainClass(mainClass))
  def withArguments(arg: String*) = this.copy(args = args ++ arg)

  override def toString: String = (scriptRunner.toSeq ++ jobRunner.toSeq ++ Seq(jar) ++ mainClass.toSeq ++ args).mkString(",")

}

object SparkStep {

  def apply(jar: String)(implicit hc: HyperionContext): SparkStep = SparkStep(
    jar = jar,
    mainClass = None,
    args = Seq(),
    scriptRunner = Option("s3://elasticmapreduce/libs/script-runner/script-runner.jar"),
    jobRunner = Option(s"${hc.scriptUri}run-spark-step.sh")
  )

}
