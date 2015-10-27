package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws._
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.adt.{HInt, HDuration, HString}
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, EmrCluster}

/**
 * Runs a MapReduce job on a cluster. The cluster can be an EMR cluster managed by AWS Data Pipeline
 * or another resource if you use TaskRunner. Use HadoopActivity when you want to run work in parallel.
 * This allows you to use the scheduling resources of the YARN framework or the MapReduce resource
 * negotiator in Hadoop 1. If you would like to run work sequentially using the Amazon EMR Step action,
 * you can still use EmrActivity.
 */
case class HadoopActivity private (
  id: PipelineObjectId,
  jarUri: HString,
  mainClass: Option[MainClass],
  argument: Seq[HString],
  hadoopQueue: Option[HString],
  preActivityTaskConfig: Option[ShellScriptConfig],
  postActivityTaskConfig: Option[ShellScriptConfig],
  inputs: Seq[S3DataNode],
  outputs: Seq[S3DataNode],
  runsOn: Resource[EmrCluster],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm],
  attemptTimeout: Option[HDuration],
  lateAfterTimeout: Option[HDuration],
  maximumRetries: Option[HInt],
  retryDelay: Option[HDuration],
  failureAndRerunMode: Option[FailureAndRerunMode]
) extends EmrActivity {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withArguments(arguments: HString*) = this.copy(argument = argument ++ arguments)
  def withHadoopQueue(queue: HString) = this.copy(hadoopQueue = Option(queue))
  def withPreActivityTaskConfig(script: ShellScriptConfig) = this.copy(preActivityTaskConfig = Option(script))
  def withPostActivityTaskConfig(script: ShellScriptConfig) = this.copy(postActivityTaskConfig = Option(script))
  def withInput(input: S3DataNode*) = this.copy(inputs = inputs ++ input)
  def withOutput(output: S3DataNode*) = this.copy(outputs = outputs ++ output)

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

  def objects: Iterable[PipelineObject] = inputs ++ outputs ++ runsOn.toSeq ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms ++ preActivityTaskConfig.toSeq ++ postActivityTaskConfig.toSeq

  lazy val serialize = AdpHadoopActivity(
    id = id,
    name = id.toOption,
    jarUri = jarUri.serialize,
    mainClass = mainClass.map(_.toString),
    argument = argument.map(_.serialize),
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

object HadoopActivity extends RunnableObject {
  def apply(jarUri: HString, mainClass: MainClass)(runsOn: Resource[EmrCluster]): HadoopActivity = apply(jarUri, Option(mainClass))(runsOn)

  def apply(jarUri: HString, mainClass: Option[MainClass] = None)(runsOn: Resource[EmrCluster]): HadoopActivity = new HadoopActivity(
    id = PipelineObjectId(HadoopActivity.getClass),
    jarUri = jarUri,
    mainClass = mainClass,
    argument = Seq.empty,
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
    failureAndRerunMode = None
  )
}
