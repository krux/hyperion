package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{WorkerGroup, Ec2Resource}

/**
 * Shell command activity that runs a given python script
 */
class PythonActivity private (
  val id: PipelineObjectId,
  val scriptUri: Option[String],
  val pythonScriptUri: Option[String],
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
  val runsOn: Either[Ec2Resource, WorkerGroup],
  val dependsOn: Seq[PipelineActivity],
  val preconditions: Seq[Precondition],
  val onFailAlarms: Seq[SnsAlarm],
  val onSuccessAlarms: Seq[SnsAlarm],
  val onLateActionAlarms: Seq[SnsAlarm],
  val attemptTimeout: Option[String],
  val lateAfterTimeout: Option[String],
  val maximumRetries: Option[Int],
  val retryDelay: Option[String],
  val failureAndRerunMode: Option[FailureAndRerunMode]
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withScriptUri(pythonScriptUri: String) = this.copy(pythonScriptUri = Option(pythonScriptUri))
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
  def withAttemptTimeout(timeout: String) = this.copy(attemptTimeout = Option(timeout))
  def withLateAfterTimeout(timeout: String) = this.copy(lateAfterTimeout = Option(timeout))
  def withMaximumRetries(retries: Int) = this.copy(maximumRetries = Option(retries))
  def withRetryDelay(delay: String) = this.copy(retryDelay = Option(delay))
  def withFailureAndRerunMode(mode: FailureAndRerunMode) = this.copy(failureAndRerunMode = Option(mode))

  def copy(
    id: PipelineObjectId = id,
    scriptUri: Option[String] = scriptUri,
    pythonScriptUri: Option[String] = pythonScriptUri,
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
    runsOn: Either[Ec2Resource, WorkerGroup] = runsOn,
    dependsOn: Seq[PipelineActivity] = dependsOn,
    preconditions: Seq[Precondition] = preconditions,
    onFailAlarms: Seq[SnsAlarm] = onFailAlarms,
    onSuccessAlarms: Seq[SnsAlarm] = onSuccessAlarms,
    onLateActionAlarms: Seq[SnsAlarm] = onLateActionAlarms,
    attemptTimeout: Option[String] = attemptTimeout,
    lateAfterTimeout: Option[String] = lateAfterTimeout,
    maximumRetries: Option[Int] = maximumRetries,
    retryDelay: Option[String] = retryDelay,
    failureAndRerunMode: Option[FailureAndRerunMode] = failureAndRerunMode
  ) = new PythonActivity(id, scriptUri, pythonScriptUri, pythonScript, pythonModule, pythonRequirements,
    pipIndexUrl, pipExtraIndexUrls, arguments, input, output, stdout, stderr, runsOn, dependsOn, preconditions,
    onFailAlarms, onSuccessAlarms, onLateActionAlarms, attemptTimeout, lateAfterTimeout, maximumRetries,
    retryDelay, failureAndRerunMode)

  override def objects: Iterable[PipelineObject] = runsOn.left.toSeq ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  private def scriptArguments = Seq(
    pythonScriptUri.map(Seq(_)),
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

object PythonActivity extends RunnableObject {

  def apply(jar: String, runsOn: Ec2Resource)(implicit hc: HyperionContext): PythonActivity = apply(jar, Left(runsOn))

  def apply(jar: String, runsOn: WorkerGroup)(implicit hc: HyperionContext): PythonActivity = apply(jar, Right(runsOn))

  private def apply(jar: String, runsOn: Either[Ec2Resource, WorkerGroup])(implicit hc: HyperionContext): PythonActivity =
    new PythonActivity(
      id = PipelineObjectId(PythonActivity.getClass),
      scriptUri = Option(s"${hc.scriptUri}activities/run-python.sh"),
      pythonScriptUri = None,
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
