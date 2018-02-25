package com.krux.hyperion.activity

import com.krux.hyperion.adt.{HInt, HString, HS3Uri}
import com.krux.hyperion.aws._
import com.krux.hyperion.common.{LegacySparkCommandRunner, Memory, PipelineObjectId, BaseFields}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.resource.{BaseEmrCluster, Resource}


/**
 * Runs a Spark job on a cluster. The cluster can be an EMR cluster managed by AWS Data Pipeline
 * or another resource if you use TaskRunner. Use SparkActivity when you want to run work in parallel.
 * This allows you to use the scheduling resources of the YARN framework or the MapReduce resource
 * negotiator in Hadoop 1. If you would like to run work sequentially using the Amazon EMR Step action,
 * you can still use SparkActivity.
 */
@deprecated("Use SparkTaskActivity instead", "5.0.0")
case class LegacySparkTaskActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[BaseEmrCluster],
  emrTaskActivityFields: EmrTaskActivityFields,
  scriptRunner: HString,
  jobRunner: HString,
  jarUri: HString,
  mainClass: MainClass,
  arguments: Seq[HString],
  hadoopQueue: Option[HString],
  inputs: Seq[S3DataNode],
  outputs: Seq[S3DataNode],
  sparkOptions: Seq[HString],
  sparkConfig: Map[HString, HString]
) extends EmrTaskActivity[BaseEmrCluster] {

  type Self = LegacySparkTaskActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[BaseEmrCluster]) = copy(activityFields = fields)
  def updateEmrTaskActivityFields(fields: EmrTaskActivityFields) = copy(emrTaskActivityFields = fields)

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

  private def sparkSettings: Seq[HString] = sparkOptions ++ sparkConfig.flatMap { case (k, v) => Seq[HString]("--conf", s"$k=$v") }

  lazy val serialize = new AdpHadoopActivity(
    id = id,
    name = name,
    jarUri = scriptRunner.serialize,
    mainClass = None,
    argument = jobRunner.serialize +: sparkSettings.map(_.serialize) ++: jarUri.serialize +: mainClass.toString +: arguments.map(_.serialize),
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

@deprecated("Use SparkTaskActivity instead", "5.0.0")
object LegacySparkTaskActivity extends RunnableObject with LegacySparkCommandRunner {

  def apply(jarUri: HS3Uri, mainClass: MainClass)(runsOn: Resource[BaseEmrCluster])(implicit hc: HyperionContext): LegacySparkTaskActivity =
    apply(jarUri.serialize, mainClass)(runsOn)

  def apply(jarUri: HString, mainClass: MainClass)(runsOn: Resource[BaseEmrCluster])(implicit hc: HyperionContext): LegacySparkTaskActivity = new LegacySparkTaskActivity(
    baseFields = BaseFields(PipelineObjectId(LegacySparkTaskActivity.getClass)),
    activityFields = ActivityFields(runsOn),
    emrTaskActivityFields = EmrTaskActivityFields(),
    jobRunner = jobRunner(runsOn),
    scriptRunner = scriptRunner(runsOn),
    jarUri = jarUri,
    mainClass = mainClass,
    arguments = Seq.empty,
    hadoopQueue = None,
    inputs = Seq.empty,
    outputs = Seq.empty,
    sparkOptions = Seq.empty,
    sparkConfig = Map.empty
  )

}
