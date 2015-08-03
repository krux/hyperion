package com.krux.hyperion.activity

import com.krux.hyperion.common.{S3Uri, PipelineObjectId, PipelineObject}
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpHiveActivity
import com.krux.hyperion.datanode.DataNode
import com.krux.hyperion.expression.Duration
import com.krux.hyperion.parameter.Parameter
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, WorkerGroup, EmrCluster}

/**
 * Runs a Hive query on an Amazon EMR cluster. HiveActivity makes it easier to set up an Amzon EMR
 * activity and automatically creates Hive tables based on input data coming in from either Amazon
 * S3 or Amazon RDS. All you need to specify is the HiveQL to run on the source data. AWS Data
 * Pipeline automatically creates Hive tables with \${input1}, \${input2}, etc. based on the input
 * fields in the Hive Activity object. For S3 inputs, the dataFormat field is used to create the
 * Hive column names. For MySQL (RDS) inputs, the column names for the SQL query are used to create
 * the Hive column names.
 */
case class HiveActivity private (
  id: PipelineObjectId,
  hiveScript: Script,
  scriptVariables: Seq[String],
  input: DataNode,
  output: DataNode,
  hadoopQueue: Option[String],
  preActivityTaskConfig: Option[ShellScriptConfig],
  postActivityTaskConfig: Option[ShellScriptConfig],
  runsOn: Resource[EmrCluster],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm],
  attemptTimeout: Option[Parameter[Duration]],
  lateAfterTimeout: Option[Parameter[Duration]],
  maximumRetries: Option[Parameter[Int]],
  retryDelay: Option[Parameter[Duration]],
  failureAndRerunMode: Option[FailureAndRerunMode]
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withScriptVariable(scriptVariable: String*) = this.copy(scriptVariables = scriptVariables ++ scriptVariable)
  def withHadoopQueue(queue: String) = this.copy(hadoopQueue = Option(queue))
  def withPreActivityTaskConfig(script: ShellScriptConfig) = this.copy(preActivityTaskConfig = Option(script))
  def withPostActivityTaskConfig(script: ShellScriptConfig) = this.copy(postActivityTaskConfig = Option(script))

  private[hyperion] def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)
  def withAttemptTimeout(timeout: Parameter[Duration]) = this.copy(attemptTimeout = Option(timeout))
  def withLateAfterTimeout(timeout: Parameter[Duration]) = this.copy(lateAfterTimeout = Option(timeout))
  def withMaximumRetries(retries: Parameter[Int]) = this.copy(maximumRetries = Option(retries))
  def withRetryDelay(delay: Parameter[Duration]) = this.copy(retryDelay = Option(delay))
  def withFailureAndRerunMode(mode: FailureAndRerunMode) = this.copy(failureAndRerunMode = Option(mode))

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ Seq(input, output) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms ++ preActivityTaskConfig.toSeq ++ postActivityTaskConfig.toSeq

  lazy val serialize = new AdpHiveActivity(
    id = id,
    name = id.toOption,
    hiveScript = hiveScript.content,
    scriptUri = hiveScript.uri.map(_.ref),
    scriptVariable = seqToOption(scriptVariables)(_.toString),
    stage = Option("true"),
    input = Option(input.ref),
    output = Option(output.ref),
    hadoopQueue = hadoopQueue,
    preActivityTaskConfig = preActivityTaskConfig.map(_.ref),
    postActivityTaskConfig = postActivityTaskConfig.map(_.ref),
    workerGroup = runsOn.asWorkerGroup.map(_.ref),
    runsOn = runsOn.asManagedResource.map(_.ref),
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref),
    attemptTimeout = attemptTimeout.map(_.toString),
    lateAfterTimeout = lateAfterTimeout.map(_.toString),
    maximumRetries = maximumRetries.map(_.toString),
    retryDelay = retryDelay.map(_.toString),
    failureAndRerunMode = failureAndRerunMode.map(_.toString)
  )
}

object HiveActivity extends RunnableObject {
  def apply(input: DataNode, output: DataNode, hiveScript: Script)(runsOn: Resource[EmrCluster]): HiveActivity =
    new HiveActivity(
      id = PipelineObjectId(HiveActivity.getClass),
      hiveScript = hiveScript,
      scriptVariables = Seq(),
      input = input,
      output = output,
      hadoopQueue = None,
      preActivityTaskConfig = None,
      postActivityTaskConfig = None,
      runsOn = runsOn,
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq(),
      attemptTimeout = None,
      lateAfterTimeout = None,
      maximumRetries = None,
      retryDelay = None,
      failureAndRerunMode = None
    )
}
