package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpHiveCopyActivity
import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.datanode.DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.adt.{HInt, HDuration, HString, HS3Uri}
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, EmrCluster}

/**
 * Runs a Hive query on an Amazon EMR cluster. HiveCopyActivity makes it easier to copy data between
 * Amazon S3 and DynamoDB. HiveCopyActivity accepts a HiveQL statement to filter input data from
 * Amazon S3 or DynomoDB at the column and row level.
 */
case class HiveCopyActivity private (
  id: PipelineObjectId,
  filterSql: Option[HString],
  generatedScriptsPath: Option[HS3Uri],
  input: DataNode,
  output: DataNode,
  hadoopQueue: Option[HString],
  preActivityTaskConfig: Option[ShellScriptConfig],
  postActivityTaskConfig: Option[ShellScriptConfig],
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
) extends PipelineActivity {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withFilterSql(filterSql: HString) = this.copy(filterSql = Option(filterSql))
  def withGeneratedScriptsPath(generatedScriptsPath: HS3Uri) = this.copy(generatedScriptsPath = Option(generatedScriptsPath))
  def withHadoopQueue(queue: HString) = this.copy(hadoopQueue = Option(queue))
  def withPreActivityTaskConfig(script: ShellScriptConfig) = this.copy(preActivityTaskConfig = Option(script))
  def withPostActivityTaskConfig(script: ShellScriptConfig) = this.copy(postActivityTaskConfig = Option(script))

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

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ Seq(input, output) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms ++ preActivityTaskConfig.toSeq ++ postActivityTaskConfig.toSeq

  lazy val serialize = AdpHiveCopyActivity(
    id = id,
    name = id.toOption,
    filterSql = filterSql.map(_.serialize),
    generatedScriptsPath = generatedScriptsPath.map(_.serialize),
    input = Option(input.ref),
    output = Option(output.ref),
    hadoopQueue = hadoopQueue.map(_.serialize),
    preActivityTaskConfig = preActivityTaskConfig.map(_.ref),
    postActivityTaskConfig = postActivityTaskConfig.map(_.ref),
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

object HiveCopyActivity extends RunnableObject {
  def apply(input: DataNode, output: DataNode)(runsOn: Resource[EmrCluster]): HiveCopyActivity =
    new HiveCopyActivity(
      id = PipelineObjectId(HiveCopyActivity.getClass),
      filterSql = None,
      generatedScriptsPath = None,
      input = input,
      output = output,
      hadoopQueue = None,
      preActivityTaskConfig = None,
      postActivityTaskConfig = None,
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
