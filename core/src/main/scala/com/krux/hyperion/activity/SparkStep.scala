package com.krux.hyperion.activity

import com.krux.hyperion.adt.{HString, HS3Uri, HInt}
import com.krux.hyperion.common.{Escapable, Memory}
import com.krux.hyperion.HyperionContext


trait SparkStep extends BaseEmrStep {

  override def mainClass = None

  def sparkJarUri: HString
  def command: HString
  def sparkMainClass: Option[MainClass]
  def sparkOptions: Seq[HString]
  def sparkConfigs: Map[HString, HString]

  def withMainClass(mc: MainClass): SparkStep
  def withArguments(args: HString*): SparkStep
  def withSparkOptions(options: HString*): SparkStep
  def withSparkConfig(key: HString, value: HString): SparkStep

  def withDriverCores(n: HInt) = withSparkOptions("--driver-cores", n.toString)
  def withDriverMemory(memory: Memory) = withSparkOptions("--driver-memory", memory.toString)

  def withExecutorCores(n: HInt) = withSparkOptions("--executor-cores", n.toString)
  def withExecutorMemory(memory: Memory) = withSparkOptions("--executor-memory", memory.toString)
  def withNumExecutors(n: HInt) = withSparkOptions("--num-executors", n.toString)
  def withTotalExecutorCores(n: HInt) = withSparkOptions("--total-executor-cores", n.toString)

  def withFiles(files: HString*) = withSparkOptions(files.flatMap(file => Seq("--files": HString, file)): _*)
  def withMaster(master: HString) = withSparkOptions("--master", master)

  override lazy val serialize: String = (
      jarUri.serialize +:
      command.serialize +:
      sparkOptions ++:
      sparkConfigs.flatMap { case (k, v) => Seq("--conf", s"$k=$v") } ++:
      sparkJarUri.serialize +:
      sparkMainClass.map(_.toString) ++:
      args
    )
    .map(x => Escapable.escape(x.toString, ',')).mkString(",")

}

object SparkStep {

  case class ScriptRunnerStep(
    sparkJarUri: HString,
    command: HString,
    sparkMainClass: Option[MainClass],
    sparkOptions: Seq[HString],
    sparkConfigs: Map[HString, HString],
    args: Seq[HString]
  ) extends SparkStep {

    def jarUri = EmrScriptRunner.toString
    def withMainClass(mc: MainClass) = copy(sparkMainClass = Option(mc))
    def withArguments(newArgs: HString*) = copy(args = args ++ newArgs)

    def withSparkOptions(options: HString*) = copy(sparkOptions = sparkOptions ++ options)
    def withSparkConfig(key: HString, value: HString) = copy(sparkConfigs = sparkConfigs + (key -> value))

  }

  case class CommandRunnerStep(
    sparkJarUri: HString,
    command: HString,
    sparkMainClass: Option[MainClass],
    sparkOptions: Seq[HString],
    sparkConfigs: Map[HString, HString],
    args: Seq[HString]
  ) extends SparkStep {

    def jarUri = EmrCommandRunner
    def withMainClass(mc: MainClass) = copy(sparkMainClass = Option(mc))
    def withArguments(newArgs: HString*) = copy(args = args ++ newArgs)

    def withSparkOptions(options: HString*) = copy(sparkOptions = sparkOptions ++ options)
    def withSparkConfig(key: HString, value: HString) = copy(sparkConfigs = sparkConfigs + (key -> value))

  }

  /**
   * Given a jar in S3, this uses EMR script runner that downloads the jar and then runs the spark
   * step. This calls scriptRunner(jarUri)
   */
  def apply(jarUri: HS3Uri)(implicit hc: HyperionContext): ScriptRunnerStep = scriptRunner(jarUri)

  /**
   * Given a jar in S3, this uses EMR script runner that downloads the jar and then runs the spark
   * step.
   */
  def scriptRunner(jarUri: HS3Uri)(implicit hc: HyperionContext): ScriptRunnerStep = ScriptRunnerStep(
    jarUri.serialize,
    s"${hc.scriptUri}run-spark-step-release-label.sh",
    None,
    Seq.empty,
    Map.empty,
    Seq.empty
  )

  /**
   * Given a jar in S3, this uses EMR script runner that downloads the jar and then runs the spark
   * step. This is for pre EMR release label 4.x.x and spark version before 2.0
   */
  def legacyScriptRunner(jarUri: HS3Uri)(implicit hc: HyperionContext): ScriptRunnerStep = ScriptRunnerStep(
    jarUri.serialize,
    s"${hc.scriptUri}run-spark-step.sh",
    None,
    Seq.empty,
    Map.empty,
    Seq.empty
  )

  /**
   * This uses the command-runner.jar in EMR release labeled versions, it requires the jar to be
   * deployed locally in the cluster.
   */
  def commandRunner(jarUri: HString): CommandRunnerStep = CommandRunnerStep(
    jarUri,
    "spark-submit",
    None,
    Seq.empty,
    Map.empty,
    Seq.empty
  )

}
