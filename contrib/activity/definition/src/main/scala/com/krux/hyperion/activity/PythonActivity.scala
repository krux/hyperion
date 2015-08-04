package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{S3Uri, PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.Duration
import com.krux.hyperion.parameter.Parameter
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

/**
 * Shell command activity that runs a given python script
 */
class PythonActivity private (
  val id: PipelineObjectId,
  val scriptUri: Option[String],
  val pythonScriptUri: Option[Parameter[S3Uri]],
  val pythonScript: Option[String],
  val pythonModule: Option[String],
  val pythonRequirements: Option[String],
  val pipIndexUrl: Option[String],
  val pipExtraIndexUrls: Seq[String],
  val arguments: Seq[String],
  val input: Seq[S3DataNode],
  val output: Seq[S3DataNode],
  val stdout: Option[String],
  val stderr: Option[String],
  val runsOn: Resource[Ec2Resource],
  val dependsOn: Seq[PipelineActivity],
  val preconditions: Seq[Precondition],
  val onFailAlarms: Seq[SnsAlarm],
  val onSuccessAlarms: Seq[SnsAlarm],
  val onLateActionAlarms: Seq[SnsAlarm],
  val attemptTimeout: Option[Parameter[Duration]],
  val lateAfterTimeout: Option[Parameter[Duration]],
  val maximumRetries: Option[Parameter[Int]],
  val retryDelay: Option[Parameter[Duration]],
  val failureAndRerunMode: Option[FailureAndRerunMode]
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withScriptUri(pythonScriptUri: Parameter[S3Uri]) = this.copy(pythonScriptUri = Option(pythonScriptUri))
  def withScript(pythonScript: String) = this.copy(pythonScript = Option(pythonScript))
  def withModule(pythonModule: String) = this.copy(pythonModule = Option(pythonModule))
  def withRequirements(pythonRequirements: String) = this.copy(pythonRequirements = Option(pythonRequirements))
  def withIndexUrl(indexUrl: String) = this.copy(pipIndexUrl = Option(indexUrl))
  def withExtraIndexUrls(indexUrl: String*) = this.copy(pipExtraIndexUrls = pipExtraIndexUrls ++ indexUrl)
  def withArguments(args: String*) = this.copy(arguments = arguments ++ args)
  def withInput(inputs: S3DataNode*) = this.copy(input = input ++ inputs)
  def withOutput(outputs: S3DataNode*) = this.copy(output = output ++ outputs)
  def withStdoutTo(out: String) = this.copy(stdout = Option(out))
  def withStderrTo(err: String) = this.copy(stderr = Option(err))

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

  def copy(
    id: PipelineObjectId = id,
    scriptUri: Option[String] = scriptUri,
    pythonScriptUri: Option[Parameter[S3Uri]] = pythonScriptUri,
    pythonScript: Option[String] = pythonScript,
    pythonModule: Option[String] = pythonModule,
    pythonRequirements: Option[String] = pythonRequirements,
    pipIndexUrl: Option[String] = pipIndexUrl,
    pipExtraIndexUrls: Seq[String] = pipExtraIndexUrls,
    arguments: Seq[String] = arguments,
    input: Seq[S3DataNode] = input,
    output: Seq[S3DataNode] = output,
    stdout: Option[String] = stdout,
    stderr: Option[String] = stderr,
    runsOn: Resource[Ec2Resource] = runsOn,
    dependsOn: Seq[PipelineActivity] = dependsOn,
    preconditions: Seq[Precondition] = preconditions,
    onFailAlarms: Seq[SnsAlarm] = onFailAlarms,
    onSuccessAlarms: Seq[SnsAlarm] = onSuccessAlarms,
    onLateActionAlarms: Seq[SnsAlarm] = onLateActionAlarms,
    attemptTimeout: Option[Parameter[Duration]] = attemptTimeout,
    lateAfterTimeout: Option[Parameter[Duration]] = lateAfterTimeout,
    maximumRetries: Option[Parameter[Int]] = maximumRetries,
    retryDelay: Option[Parameter[Duration]] = retryDelay,
    failureAndRerunMode: Option[FailureAndRerunMode] = failureAndRerunMode
  ) = new PythonActivity(id, scriptUri, pythonScriptUri, pythonScript, pythonModule, pythonRequirements,
    pipIndexUrl, pipExtraIndexUrls, arguments, input, output, stdout, stderr, runsOn, dependsOn, preconditions,
    onFailAlarms, onSuccessAlarms, onLateActionAlarms, attemptTimeout, lateAfterTimeout, maximumRetries,
    retryDelay, failureAndRerunMode)

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  private def scriptArguments = Seq(
    pythonScriptUri.map(Seq(_).map(_.toString)),
    pythonScript.map(Seq(_)),
    pythonRequirements.map(Seq("-r", _)),
    pythonModule.map(Seq("-m", _)),
    pipIndexUrl.map(Seq("-i", _))
  ).flatten.flatten ++ pipExtraIndexUrls.flatMap(Seq("--extra-index-url", _)) ++ Seq("--") ++ arguments

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = scriptUri,
    scriptArgument = Option(scriptArguments),
    stdout = stdout,
    stderr = stderr,
    stage = Option("true"),
    input = seqToOption(input)(_.ref),
    output = seqToOption(output)(_.ref),
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

object PythonActivity extends RunnableObject {

  def apply(pythonScriptUri: Parameter[S3Uri])(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): PythonActivity =
    new PythonActivity(
      id = PipelineObjectId(PythonActivity.getClass),
      scriptUri = Option(s"${hc.scriptUri}activities/run-python.sh"),
      pythonScriptUri = Option(pythonScriptUri),
      pythonScript = None,
      pythonModule = None,
      pythonRequirements = None,
      pipIndexUrl = None,
      pipExtraIndexUrls = Seq(),
      arguments = Seq(),
      input = Seq(),
      output = Seq(),
      stdout = None,
      stderr = None,
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
