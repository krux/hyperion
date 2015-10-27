package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws._
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.adt.{HInt, HDuration, HString}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{SparkCluster, Resource}

/**
 * Runs a Spark job on a cluster. The cluster can be an EMR cluster managed by AWS Data Pipeline
 * or another resource if you use TaskRunner. Use SparkJobActivity when you want to run work in parallel.
 * This allows you to use the scheduling resources of the YARN framework or the MapReduce resource
 * negotiator in Hadoop 1. If you would like to run work sequentially using the Amazon EMR Step action,
 * you can still use SparkActivity.
 */
class SparkJobActivity private (
  val id: PipelineObjectId,
  val scriptRunner: HString,
  val jobRunner: HString,
  val jarUri: HString,
  val mainClass: MainClass,
  val args: Seq[HString],
  val hadoopQueue: Option[HString],
  val preActivityTaskConfig: Option[ShellScriptConfig],
  val postActivityTaskConfig: Option[ShellScriptConfig],
  val inputs: Seq[S3DataNode],
  val outputs: Seq[S3DataNode],
  val runsOn: Resource[SparkCluster],
  val dependsOn: Seq[PipelineActivity],
  val preconditions: Seq[Precondition],
  val onFailAlarms: Seq[SnsAlarm],
  val onSuccessAlarms: Seq[SnsAlarm],
  val onLateActionAlarms: Seq[SnsAlarm],
  val attemptTimeout: Option[HDuration],
  val lateAfterTimeout: Option[HDuration],
  val maximumRetries: Option[HInt],
  val retryDelay: Option[HDuration],
  val failureAndRerunMode: Option[FailureAndRerunMode],
  val sparkOptions: Seq[HString],
  val sparkConfig: Map[HString, HString]
) extends EmrActivity {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withArguments(argument: HString*) = this.copy(args = args ++ argument)
  def withHadoopQueue(queue: HString) = this.copy(hadoopQueue = Option(queue))
  def withPreActivityTaskConfig(script: ShellScriptConfig) = this.copy(preActivityTaskConfig = Option(script))
  def withPostActivityTaskConfig(script: ShellScriptConfig) = this.copy(postActivityTaskConfig = Option(script))
  def withInput(input: S3DataNode*) = this.copy(inputs = inputs ++ input)
  def withOutput(output: S3DataNode*) = this.copy(outputs = outputs ++ output)
  def withSparkOption(option: HString*) = this.copy(sparkOptions = sparkOptions ++ option)
  def withSparkConfig(key: HString, value: HString) = this.copy(sparkConfig = sparkConfig + (key -> value))

  private[hyperion] def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)
  def withAttemptTimeout(timeout: HDuration) = this.copy(attemptTimeout = Option(timeout))
  def withLateAfterTimeout(timeout: HDuration) = this.copy(lateAfterTimeout = Option(timeout))
  def withMaximumRetries(retries: HInt) = this.copy(maximumRetries = Option(retries))
  def withRetryDelay(delay: HDuration) = this.copy(retryDelay = Option(delay))
  def withFailureAndRerunMode(mode: FailureAndRerunMode) = this.copy(failureAndRerunMode = Option(mode))

  def copy(id: PipelineObjectId = id,
    scriptRunner: HString = scriptRunner,
    jobRunner: HString = jobRunner,
    jarUri: HString = jarUri,
    mainClass: MainClass = mainClass,
    args: Seq[HString] = args,
    hadoopQueue: Option[HString] = hadoopQueue,
    preActivityTaskConfig: Option[ShellScriptConfig] = preActivityTaskConfig,
    postActivityTaskConfig: Option[ShellScriptConfig] = postActivityTaskConfig,
    inputs: Seq[S3DataNode] = inputs,
    outputs: Seq[S3DataNode] = outputs,
    runsOn: Resource[SparkCluster] = runsOn,
    dependsOn: Seq[PipelineActivity] = dependsOn,
    preconditions: Seq[Precondition] = preconditions,
    onFailAlarms: Seq[SnsAlarm] = onFailAlarms,
    onSuccessAlarms: Seq[SnsAlarm] = onSuccessAlarms,
    onLateActionAlarms: Seq[SnsAlarm] = onLateActionAlarms,
    attemptTimeout: Option[HDuration] = attemptTimeout,
    lateAfterTimeout: Option[HDuration] = lateAfterTimeout,
    maximumRetries: Option[HInt] = maximumRetries,
    retryDelay: Option[HDuration] = retryDelay,
    failureAndRerunMode: Option[FailureAndRerunMode] = failureAndRerunMode,
    sparkOptions: Seq[HString] = sparkOptions,
    sparkConfig: Map[HString, HString] = sparkConfig
  ): SparkJobActivity = new SparkJobActivity(id, scriptRunner, jobRunner, jarUri, mainClass, args,
    hadoopQueue, preActivityTaskConfig, postActivityTaskConfig, inputs, outputs, runsOn, dependsOn,
    preconditions, onFailAlarms, onSuccessAlarms, onLateActionAlarms, attemptTimeout, lateAfterTimeout,
    maximumRetries, retryDelay, failureAndRerunMode, sparkOptions, sparkConfig)

  def objects: Iterable[PipelineObject] = inputs ++ outputs ++ runsOn.toSeq ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms ++ preActivityTaskConfig.toSeq ++ postActivityTaskConfig.toSeq

  private def sparkSettings: Seq[HString] = sparkOptions ++ sparkConfig.flatMap { case (k, v) => Seq[HString]("--conf", s"$k=$v") }

  lazy val serialize = AdpHadoopActivity(
    id = id,
    name = id.toOption,
    jarUri = scriptRunner.serialize,
    mainClass = None,
    argument = jobRunner.serialize +: sparkSettings.map(_.serialize) ++: jarUri.serialize +: mainClass.toString +: args.map(_.serialize),
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
    failureAndRerunMode = failureAndRerunMode.map(_.serialize)
  )

}

object SparkJobActivity extends RunnableObject {
  def apply(jarUri: HString, mainClass: MainClass)(runsOn: Resource[SparkCluster])(implicit hc: HyperionContext): SparkJobActivity = new SparkJobActivity(
    id = PipelineObjectId(SparkJobActivity.getClass),
    scriptRunner = "s3://elasticmapreduce/libs/script-runner/script-runner.jar",
    jobRunner = s"${hc.scriptUri}run-spark-step.sh",
    jarUri = jarUri,
    mainClass = mainClass,
    args = Seq.empty,
    hadoopQueue = None,
    preActivityTaskConfig = None,
    postActivityTaskConfig = None,
    inputs = Seq.empty,
    outputs = Seq.empty,
    runsOn = runsOn,
    dependsOn = Seq.empty,
    preconditions = Seq.empty,
    onFailAlarms = Seq.empty,
    onSuccessAlarms = Seq.empty,
    onLateActionAlarms = Seq.empty,
    attemptTimeout = None,
    lateAfterTimeout = None,
    maximumRetries = None,
    retryDelay = None,
    failureAndRerunMode = None,
    sparkOptions = Seq.empty,
    sparkConfig = Map.empty
  )
}
