package com.krux.hyperion.activity

import com.krux.hyperion.adt.{HInt, HString, HS3Uri}
import com.krux.hyperion.aws._
import com.krux.hyperion.common.{Memory, PipelineObjectId, BaseFields}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.resource.{BaseEmrCluster, EmrCluster, LegacyEmrCluster, Resource}


/**
 * Runs a Spark job on a cluster. The cluster can be an EMR cluster managed by AWS Data Pipeline
 * or another resource if you use TaskRunner. Use SparkActivity when you want to run work in parallel.
 * This allows you to use the scheduling resources of the YARN framework or the MapReduce resource
 * negotiator in Hadoop 1. If you would like to run work sequentially using the Amazon EMR Step action,
 * you can still use SparkActivity.
 */
case class SparkTaskActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[BaseEmrCluster],
  emrTaskActivityFields: EmrTaskActivityFields,
  jarUri: HString,
  sparkJarUri: HString,
  command: HString,
  sparkMainClass: Option[MainClass],
  arguments: Seq[HString],
  hadoopQueue: Option[HString],
  inputs: Seq[S3DataNode],
  outputs: Seq[S3DataNode],
  sparkOptions: Seq[HString],
  sparkConfig: Map[HString, HString]
) extends EmrTaskActivity[BaseEmrCluster] {

  type Self = SparkTaskActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[BaseEmrCluster]) = copy(activityFields = fields)
  def updateEmrTaskActivityFields(fields: EmrTaskActivityFields) = copy(emrTaskActivityFields = fields)

  def mainClass = None
  def withMainClass(mc: MainClass) = copy(sparkMainClass = Option(mc))
  def withArguments(args: HString*) = copy(arguments = arguments ++ args)
  def withHadoopQueue(queue: HString) = copy(hadoopQueue = Option(queue))
  def withInput(input: S3DataNode*) = copy(inputs = inputs ++ input)
  def withOutput(output: S3DataNode*) = copy(outputs = outputs ++ output)
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

  override def objects = inputs ++ outputs ++ super.objects

  private def isCommandRunner = jarUri.toString.endsWith(EmrCommandRunner)

  private def sparkSettings: Seq[HString] = sparkOptions ++
    sparkConfig.flatMap { case (k, v) => Seq[HString]("--conf", s"$k=$v") } ++
    sparkMainClass.filter(_ => isCommandRunner).toSeq.flatMap( main => Seq[HString]("--class", main.toString))

  lazy val serialize = new AdpHadoopActivity(
    id = id,
    name = name,
    jarUri = jarUri.serialize,
    mainClass = mainClass,
    argument = command.serialize +:
      sparkSettings.map(_.serialize) ++:
      sparkJarUri.serialize +: sparkMainClass.filterNot(_ => isCommandRunner).map(_.toString) ++: arguments.map(_.serialize),
    hadoopQueue = hadoopQueue.map(_.serialize),
    preActivityTaskConfig = preActivityTaskConfig.map(_.ref),
    postActivityTaskConfig = postActivityTaskConfig.map(_.ref),
    input = seqToOption(inputs)(_.ref),
    output = seqToOption(outputs)(_.ref),
    workerGroup = runsOn.asWorkerGroup.map(_.ref),
    runsOn = runsOn.asManagedResource.map(_.ref),
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref),
    attemptTimeout = attemptTimeout.map(_.serialize),
    lateAfterTimeout = lateAfterTimeout.map(_.serialize),
    maximumRetries = maximumRetries.map(_.serialize),
    retryDelay = retryDelay.map(_.serialize),
    failureAndRerunMode = failureAndRerunMode.map(_.serialize),
    maxActiveInstances = maxActiveInstances.map(_.serialize)
  )

}

object SparkTaskActivity extends RunnableObject {

  def scriptRunner(jarUri: HS3Uri)(runsOn: Resource[EmrCluster])(implicit hc: HyperionContext) = new SparkTaskActivity(
    baseFields = BaseFields(PipelineObjectId(SparkTaskActivity.getClass)),
    activityFields = ActivityFields(runsOn),
    emrTaskActivityFields = EmrTaskActivityFields(),
    jarUri = EmrScriptRunner.toString,
    sparkJarUri = jarUri.serialize,
    command = s"${hc.scriptUri}run-spark-step-release-label.sh",
    sparkMainClass = None,
    arguments = Seq.empty,
    hadoopQueue = None,
    inputs = Seq.empty,
    outputs = Seq.empty,
    sparkOptions = Seq.empty,
    sparkConfig = Map.empty
  )

  def apply(jarUri: HS3Uri)(runsOn: Resource[EmrCluster])(implicit hc: HyperionContext) =
    scriptRunner(jarUri)(runsOn)

  /**
   * Use this one to deploy spark task actvity on pre emr-4.0.0 clusters
   */
  def legacyScriptRunner(jarUri: HS3Uri)(runsOn: Resource[LegacyEmrCluster])(implicit hc: HyperionContext) = new SparkTaskActivity(
    baseFields = BaseFields(PipelineObjectId(SparkTaskActivity.getClass)),
    activityFields = ActivityFields(runsOn),
    emrTaskActivityFields = EmrTaskActivityFields(),
    jarUri = EmrScriptRunner.toString,
    sparkJarUri = jarUri.serialize,
    command = s"${hc.scriptUri}run-spark-step.sh",
    sparkMainClass = None,
    arguments = Seq.empty,
    hadoopQueue = None,
    inputs = Seq.empty,
    outputs = Seq.empty,
    sparkOptions = Seq.empty,
    sparkConfig = Map.empty
  )

  def commandRunner(jarUri: HString)(runsOn: Resource[EmrCluster]) = new SparkTaskActivity(
    baseFields = BaseFields(PipelineObjectId(SparkTaskActivity.getClass)),
    activityFields = ActivityFields(runsOn),
    emrTaskActivityFields = EmrTaskActivityFields(),
    jarUri = EmrHadoopJarsDir.resolve(EmrCommandRunner).toString,
    sparkJarUri = jarUri,
    command = "spark-submit",
    sparkMainClass = None,
    arguments = Seq.empty,
    hadoopQueue = None,
    inputs = Seq.empty,
    outputs = Seq.empty,
    sparkOptions = Seq.empty,
    sparkConfig = Map.empty
  )

}
