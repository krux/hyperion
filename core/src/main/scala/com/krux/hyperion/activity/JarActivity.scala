package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.{HInt, HDuration, HS3Uri, HBoolean, HString}
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{S3Uri, PipelineObjectId, PipelineObject}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

/**
 * Shell command activity that runs a given Jar
 */
case class JarActivity private (
  id: PipelineObjectId,
  jarUri: HS3Uri,
  scriptUri: Option[HS3Uri],
  mainClass: Option[MainClass],
  options: Seq[HString],
  arguments: Seq[HString],
  stdout: Option[HString],
  stderr: Option[HString],
  stage: Option[HBoolean],
  input: Seq[S3DataNode],
  output: Seq[S3DataNode],
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

  def withMainClass(mainClass: MainClass) = this.copy(mainClass = Option(mainClass))
  def withOptions(opts: HString*) = this.copy(options = options ++ opts)
  def withArguments(args: HString*) = this.copy(arguments = arguments ++ args)
  def withStdoutTo(out: HString) = this.copy(stdout = Option(out))
  def withStderrTo(err: HString) = this.copy(stderr = Option(err))
  def withInput(inputs: S3DataNode*) = this.copy(input = input ++ inputs, stage = Option(HBoolean.True))
  def withOutput(outputs: S3DataNode*) = this.copy(output = output ++ outputs, stage = Option(HBoolean.True))

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

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = scriptUri.map(_.serialize),
    scriptArgument = Option(jarUri.serialize +: options.map(_.serialize) ++: mainClass.map(_.toString).toSeq ++: arguments.map(_.serialize)),
    stdout = stdout.map(_.serialize),
    stderr = stderr.map(_.serialize),
    stage = stage.map(_.serialize),
    input = seqToOption(input)(_.ref),
    output = seqToOption(output)(_.ref),
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

object JarActivity extends RunnableObject {

  def apply(jarUri: HS3Uri)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): JarActivity =
    new JarActivity(
      id = PipelineObjectId(JarActivity.getClass),
      jarUri = jarUri,
      scriptUri = Option(S3Uri(s"${hc.scriptUri}activities/run-jar.sh"): HS3Uri),
      mainClass = None,
      options = Seq.empty,
      arguments = Seq.empty,
      stdout = None,
      stderr = None,
      stage = None,
      input = Seq.empty,
      output = Seq.empty,
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
