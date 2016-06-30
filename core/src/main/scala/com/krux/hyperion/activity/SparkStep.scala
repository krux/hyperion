package com.krux.hyperion.activity

import com.krux.hyperion.adt.{ HS3Uri, HString, HInt }
import com.krux.hyperion.common.{ Memory, Escapable }
import com.krux.hyperion.HyperionContext

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
) extends Escapable {

  def withMainClass(mainClass: MainClass) = copy(mainClass = Option(mainClass))
  def withArguments(arg: HString*) = copy(args = args ++ arg)
  def withSparkOption(option: HString*) = copy(sparkOptions = sparkOptions ++ option)
  def withSparkConfig(key: HString, value: HString) = copy(sparkConfig = sparkConfig + (key -> value))

  def withDriverCores(n: HInt) = withSparkOption("--driver-cores", n.toString)
  def withDriverMemory(memory: Memory) = withSparkOption("--driver-memory", memory.toString)

  def withExecutorCores(n: HInt) = withSparkOption("--executor-cores", n.toString)
  def withExecutorMemory(memory: Memory) = withSparkOption("--executor-memory", memory.toString)
  def withNumExecutors(n: HInt) = withSparkOption("--num-executors", n.toString)
  def withTotalExecutorCores(n: HInt) = withSparkOption("--total-executor-cores", n.toString)

  def withFiles(files: HString*) = withSparkOption(files.flatMap(file => Seq("--files": HString, file)): _*)
  def withMaster(master: HString) = withSparkOption("--master", master)

  def serialize: String = Seq(
    Seq(scriptRunner, jobRunner),
    sparkOptions,
    sparkConfig.flatMap { case (k, v) => Seq("--conf", s"$k=$v") }.toSeq,
    Seq(jarUri.serialize),
    mainClass.map(_.toString).toSeq,
    args
  ).flatten.map(x => escape(x.toString, ',')).mkString(",")

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
