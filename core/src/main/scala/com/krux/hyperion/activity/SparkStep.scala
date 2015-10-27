package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.adt.{HS3Uri, HString}

/**
 * A Spark step that runs on Spark Cluster
 */
case class SparkStep private (
  jarUri: HS3Uri,
  mainClass: Option[MainClass],
  args: Seq[HString],
  scriptRunner: HString,
  jobRunner: HString,
  sparkOptions: Seq[HString],
  sparkConfig: Map[HString, HString]
) {

  def withMainClass(mainClass: MainClass) = this.copy(mainClass = Option(mainClass))
  def withArguments(arg: HString*) = this.copy(args = args ++ arg)
  def withSparkOption(option: HString*) = this.copy(sparkOptions = sparkOptions ++ option)
  def withSparkConfig(key: HString, value: HString) = this.copy(sparkConfig = sparkConfig + (key -> value))

  def serialize: String = Seq(
    Seq(scriptRunner, jobRunner),
    sparkOptions,
    sparkConfig.flatMap { case (k, v) => Seq("--conf", s"$k=$v") }.toSeq,
    Seq(jarUri.serialize),
    mainClass.map(_.toString).toSeq,
    args
  ).flatten.mkString(",")

  override def toString: String = serialize
}

object SparkStep {

  def apply(jarUri: HS3Uri)(implicit hc: HyperionContext): SparkStep = SparkStep(
    jarUri = jarUri,
    mainClass = None,
    args = Seq.empty,
    scriptRunner = "s3://elasticmapreduce/libs/script-runner/script-runner.jar",
    jobRunner = s"${hc.scriptUri}run-spark-step.sh",
    sparkOptions = Seq.empty,
    sparkConfig = Map.empty
  )

}
