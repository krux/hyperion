package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.common.{Memory, S3Uri}
import com.krux.hyperion.parameter.Parameter

/**
 * A Spark step that runs on Spark Cluster
 */
case class SparkStep private (
  jarUri: Parameter[S3Uri],
  mainClass: Option[MainClass],
  args: Seq[String],
  scriptRunner: String,
  jobRunner: String,
  sparkOptions: Seq[String],
  sparkConfig: Map[String, String]
) {

  def withMainClass(mainClass: MainClass) = this.copy(mainClass = Option(mainClass))
  def withArguments(arg: String*) = this.copy(args = args ++ arg)
  def withSparkOption(option: String*) = this.copy(sparkOptions = sparkOptions ++ option)
  def withSparkConfig(key: String, value: String) = this.copy(sparkConfig = sparkConfig + (key -> value))

  def withDriverCores(n: Int) = this.withSparkOption("--driver-cores", n.toString)
  def withDriverMemory(memory: Memory) = this.withSparkOption("--driver-memory", memory.toString)

  def withExecutorCores(n: Int) = this.withSparkOption("--executor-cores", n.toString)
  def withExecutorMemory(memory: Memory) = this.withSparkOption("--executor-memory", memory.toString)
  def withNumExecutors(n: Int) = this.withSparkOption("--num-executors", n.toString)
  def withTotalExecutorCores(n: Int) = this.withSparkOption("--total-executor-cores", n.toString)

  def withFiles(files: String*) = this.withSparkOption(files.flatMap(file => Seq("--files", file)): _*)
  def withMaster(master: String) = this.withSparkOption("--master", master)

  override def toString: String = Seq(
    Seq(scriptRunner, jobRunner),
    sparkOptions,
    sparkConfig.flatMap { case (k, v) => Seq("--conf", s"$k=$v") }.toSeq,
    Seq(jarUri.toString),
    mainClass.map(_.toString).toSeq,
    args
  ).flatten.mkString(",")

}

object SparkStep {

  def apply(jarUri: Parameter[S3Uri])(implicit hc: HyperionContext): SparkStep = SparkStep(
    jarUri = jarUri,
    mainClass = None,
    args = Seq.empty,
    scriptRunner = "s3://elasticmapreduce/libs/script-runner/script-runner.jar",
    jobRunner = s"${hc.scriptUri}run-spark-step.sh",
    sparkOptions = Seq.empty,
    sparkConfig = Map.empty
  )

}
