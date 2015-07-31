package com.krux.hyperion.activity

import com.krux.hyperion.common.{S3Uri, PipelineObjectId, PipelineObject}
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpHiveActivity
import com.krux.hyperion.datanode.DataNode
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{WorkerGroup, EmrCluster}

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
  hiveScript: Either[S3Uri, String],
  scriptVariables: Seq[String],
  stage: Option[Boolean],
  input: Option[DataNode],
  output: Option[DataNode],
  hadoopQueue: Option[String],
  preActivityTaskConfig: Option[ShellScriptConfig],
  postActivityTaskConfig: Option[ShellScriptConfig],
  runsOn: Either[EmrCluster, WorkerGroup],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm],
  attemptTimeout: Option[String],
  lateAfterTimeout: Option[String],
  maximumRetries: Option[Int],
  retryDelay: Option[String],
  failureAndRerunMode: Option[FailureAndRerunMode]
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withHiveScript(hiveScript: String) = this.copy(hiveScript = Right(hiveScript))
  def withScriptUri(scriptUri: S3Uri) = this.copy(hiveScript = Left(scriptUri))
  def withScriptVariable(scriptVariable: String*) = this.copy(scriptVariables = scriptVariables ++ scriptVariable)
  def withInput(in: DataNode) = this.copy(input = Option(in), stage = Option(true))
  def withOutput(out: DataNode) = this.copy(output = Option(out), stage = Option(true))
  def withHadoopQueue(queue: String) = this.copy(hadoopQueue = Option(queue))
  def withPreActivityTaskConfig(script: ShellScriptConfig) = this.copy(preActivityTaskConfig = Option(script))
  def withPostActivityTaskConfig(script: ShellScriptConfig) = this.copy(postActivityTaskConfig = Option(script))

  private[hyperion] def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)
  def withAttemptTimeout(timeout: String) = this.copy(attemptTimeout = Option(timeout))
  def withLateAfterTimeout(timeout: String) = this.copy(lateAfterTimeout = Option(timeout))
  def withMaximumRetries(retries: Int) = this.copy(maximumRetries = Option(retries))
  def withRetryDelay(delay: String) = this.copy(retryDelay = Option(delay))
  def withFailureAndRerunMode(mode: FailureAndRerunMode) = this.copy(failureAndRerunMode = Option(mode))

  override def objects: Iterable[PipelineObject] = runsOn.left.toSeq ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms ++ preActivityTaskConfig.toSeq ++ postActivityTaskConfig.toSeq

  lazy val serialize = new AdpHiveActivity(
    id = id,
    name = id.toOption,
    hiveScript = hiveScript.right.toOption,
    scriptUri = hiveScript.left.toOption.map(_.ref),
    scriptVariable = seqToOption(scriptVariables)(_.toString),
    stage = stage.map(_.toString),
    input = input.map(_.ref),
    output = output.map(_.ref),
    hadoopQueue = hadoopQueue,
    preActivityTaskConfig = preActivityTaskConfig.map(_.ref),
    postActivityTaskConfig = postActivityTaskConfig.map(_.ref),
    workerGroup = runsOn.right.toOption.map(_.ref),
    runsOn = runsOn.left.toOption.map(_.ref),
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref),
    attemptTimeout = attemptTimeout,
    lateAfterTimeout = lateAfterTimeout,
    maximumRetries = maximumRetries.map(_.toString),
    retryDelay = retryDelay,
    failureAndRerunMode = failureAndRerunMode.map(_.toString)
  )
}

object HiveActivity extends RunnableObject {
  def apply(hiveScript: Either[S3Uri, String], runsOn: EmrCluster): HiveActivity = apply(hiveScript, Left(runsOn))

  def apply(hiveScript: Either[S3Uri, String], runsOn: WorkerGroup): HiveActivity = apply(hiveScript, Right(runsOn))

  private def apply(hiveScript: Either[S3Uri, String], runsOn: Either[EmrCluster, WorkerGroup]): HiveActivity =
    new HiveActivity(
      id = PipelineObjectId(HiveActivity.getClass),
      hiveScript = hiveScript,
      scriptVariables = Seq(),
      stage = None,
      input = None,
      output = None,
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
