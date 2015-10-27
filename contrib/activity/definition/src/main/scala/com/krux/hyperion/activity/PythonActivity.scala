package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.adt.{HInt, HDuration, HS3Uri, HString, HBoolean, HType}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

/**
 * Shell command activity that runs a given python script
 */
class PythonActivity private (
  val id: PipelineObjectId,
  val scriptUri: Option[HString],
  val pythonScriptUri: Option[HS3Uri],
  val pythonScript: Option[HString],
  val pythonModule: Option[HString],
  val pythonRequirements: Option[HString],
  val pipIndexUrl: Option[HString],
  val pipExtraIndexUrls: Seq[HString],
  val arguments: Seq[HString],
  val input: Seq[S3DataNode],
  val output: Seq[S3DataNode],
  val stdout: Option[HString],
  val stderr: Option[HString],
  val runsOn: Resource[Ec2Resource],
  val dependsOn: Seq[PipelineActivity],
  val preconditions: Seq[Precondition],
  val onFailAlarms: Seq[SnsAlarm],
  val onSuccessAlarms: Seq[SnsAlarm],
  val onLateActionAlarms: Seq[SnsAlarm],
  val attemptTimeout: Option[HDuration],
  val lateAfterTimeout: Option[HDuration],
  val maximumRetries: Option[HInt],
  val retryDelay: Option[HDuration],
  val failureAndRerunMode: Option[FailureAndRerunMode]
) extends PipelineActivity {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withScriptUri(pythonScriptUri: HS3Uri) = this.copy(pythonScriptUri = Option(pythonScriptUri))
  def withScript(pythonScript: HString) = this.copy(pythonScript = Option(pythonScript))
  def withModule(pythonModule: HString) = this.copy(pythonModule = Option(pythonModule))
  def withRequirements(pythonRequirements: HString) = this.copy(pythonRequirements = Option(pythonRequirements))
  def withIndexUrl(indexUrl: HString) = this.copy(pipIndexUrl = Option(indexUrl))
  def withExtraIndexUrls(indexUrl: HString*) = this.copy(pipExtraIndexUrls = pipExtraIndexUrls ++ indexUrl)
  def withArguments(args: HString*) = this.copy(arguments = arguments ++ args)
  def withInput(inputs: S3DataNode*) = this.copy(input = input ++ inputs)
  def withOutput(outputs: S3DataNode*) = this.copy(output = output ++ outputs)
  def withStdoutTo(out: HString) = this.copy(stdout = Option(out))
  def withStderrTo(err: HString) = this.copy(stderr = Option(err))

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

  def copy(
    id: PipelineObjectId = id,
    scriptUri: Option[HString] = scriptUri,
    pythonScriptUri: Option[HS3Uri] = pythonScriptUri,
    pythonScript: Option[HString] = pythonScript,
    pythonModule: Option[HString] = pythonModule,
    pythonRequirements: Option[HString] = pythonRequirements,
    pipIndexUrl: Option[HString] = pipIndexUrl,
    pipExtraIndexUrls: Seq[HString] = pipExtraIndexUrls,
    arguments: Seq[HString] = arguments,
    input: Seq[S3DataNode] = input,
    output: Seq[S3DataNode] = output,
    stdout: Option[HString] = stdout,
    stderr: Option[HString] = stderr,
    runsOn: Resource[Ec2Resource] = runsOn,
    dependsOn: Seq[PipelineActivity] = dependsOn,
    preconditions: Seq[Precondition] = preconditions,
    onFailAlarms: Seq[SnsAlarm] = onFailAlarms,
    onSuccessAlarms: Seq[SnsAlarm] = onSuccessAlarms,
    onLateActionAlarms: Seq[SnsAlarm] = onLateActionAlarms,
    attemptTimeout: Option[HDuration] = attemptTimeout,
    lateAfterTimeout: Option[HDuration] = lateAfterTimeout,
    maximumRetries: Option[HInt] = maximumRetries,
    retryDelay: Option[HDuration] = retryDelay,
    failureAndRerunMode: Option[FailureAndRerunMode] = failureAndRerunMode
  ) = new PythonActivity(id, scriptUri, pythonScriptUri, pythonScript, pythonModule, pythonRequirements,
    pipIndexUrl, pipExtraIndexUrls, arguments, input, output, stdout, stderr, runsOn, dependsOn, preconditions,
    onFailAlarms, onSuccessAlarms, onLateActionAlarms, attemptTimeout, lateAfterTimeout, maximumRetries,
    retryDelay, failureAndRerunMode)

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  private def scriptArguments: Seq[HType] = Seq(
    pythonScriptUri.map(Seq(_)),
    pythonScript.map(Seq(_)),
    pythonRequirements.map(Seq[HString]("-r", _)),
    pythonModule.map(Seq[HString]("-m", _)),
    pipIndexUrl.map(Seq[HString]("-i", _))
  ).flatten.flatten ++ pipExtraIndexUrls.flatMap(Seq[HString]("--extra-index-url", _)) ++ Seq[HString]("--") ++ arguments

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = scriptUri.map(_.serialize),
    scriptArgument = Option(scriptArguments.map(_.serialize)),
    stdout = stdout.map(_.serialize),
    stderr = stderr.map(_.serialize),
    stage = Option(HBoolean.True.serialize),
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

object PythonActivity extends RunnableObject {

  def apply(pythonScriptUri: HS3Uri)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): PythonActivity =
    new PythonActivity(
      id = PipelineObjectId(PythonActivity.getClass),
      scriptUri = Option(s"${hc.scriptUri}activities/run-python.sh": HString),
      pythonScriptUri = Option(pythonScriptUri),
      pythonScript = None,
      pythonModule = None,
      pythonRequirements = None,
      pipIndexUrl = None,
      pipExtraIndexUrls = Seq.empty,
      arguments = Seq.empty,
      input = Seq.empty,
      output = Seq.empty,
      stdout = None,
      stderr = None,
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
