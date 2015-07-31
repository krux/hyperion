package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws._
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{WorkerGroup, EmrCluster}

case class HadoopActivity(
  id: PipelineObjectId,
  jarUri: String,
  mainClass: Option[String],
  argument: Seq[String],
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
) extends EmrActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withMainClass(mainClass: Any) = this.copy(mainClass = ActivityHelper.getMainClass(mainClass))
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

  override def objects: Iterable[PipelineObject] = runsOn.left.toSeq ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpHadoopActivity(
    id = id,
    name = id.toOption,
    jarUri = jarUri,
    mainClass = mainClass,
    argument: Seq[String],
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

object HadoopActivity {
  def apply(jarUri: String, runsOn: EmrCluster): HadoopActivity = apply(jarUri, Left(runsOn))

  def apply(jarUri: String, runsOn: WorkerGroup): HadoopActivity = apply(jarUri, Right(runsOn))

  private def apply(jarUri: String, runsOn: Either[EmrCluster, WorkerGroup]): HadoopActivity = new HadoopActivity(
    id = PipelineObjectId(HadoopActivity.getClass),
    jarUri = jarUri,
    mainClass = None,
    argument = Seq(),
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
