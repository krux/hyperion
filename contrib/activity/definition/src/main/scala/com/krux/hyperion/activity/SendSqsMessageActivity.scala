package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.{HInt, HDuration, HString, HBoolean, HType}
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, S3Uri, PipelineObjectId}
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Ec2Resource, Resource}

case class SendSqsMessageActivity private (
  id: PipelineObjectId,
  scriptUri: Option[S3Uri],
  jarUri: HString,
  mainClass: HString,
  queue: HString,
  message: HString,
  region: Option[HString],
  delay: Option[HInt],
  attributes: Map[HString, (HString, HString)],
  runsOn: Resource[Ec2Resource],
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

  def withRegion(region: HString) = this.copy(region = Option(region))
  def withDelaySeconds(delay: HInt) = this.copy(delay = Option(delay))
  def withAttribute(key: HString, value: HString, dataType: HString = "String") = {
    val attribute = (key, (dataType, value))
    this.copy(attributes = attributes + attribute)
  }

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

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  private def arguments: Seq[HType] = Seq(
    Option(Seq[HString]("--queue", queue)),
    region.map(Seq[HString]("--region", _)),
    if (attributes.nonEmpty) Option(Seq[HString]("--attributes", attributes.toSeq.map { case (k, (t, v)) => s"$k:$t=$v"}.mkString(","))) else None,
    delay.map(d => Seq[HType]("--delay", d)),
    Option(Seq[HString]("--message", message))
  ).flatten.flatten

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = scriptUri.map(_.ref),
    scriptArgument = Option((jarUri +: mainClass +: arguments).map(_.serialize)),
    stdout = None,
    stderr = None,
    stage = Option(HBoolean.False.serialize),
    input = None,
    output = None,
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

object SendSqsMessageActivity extends RunnableObject {
  def apply(queue: String,
    message: String)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): SendSqsMessageActivity =
    new SendSqsMessageActivity(
      id = PipelineObjectId(SendSqsMessageActivity.getClass),
      scriptUri = Option(S3Uri(s"${hc.scriptUri}activities/run-jar.sh")),
      jarUri = s"${hc.scriptUri}activities/hyperion-notification-activity-current-assembly.jar",
      mainClass = "com.krux.hyperion.contrib.activity.notification.SendSqsMessage",
      queue = queue,
      message = message,
      region = None,
      delay = None,
      attributes = Map.empty,
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
