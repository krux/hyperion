package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.Duration
import com.krux.hyperion.parameter.Parameter
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

class SplitMergeFilesActivity private (
  val id: PipelineObjectId,
  val scriptUri: Option[String],
  val jarUri: String,
  val mainClass: String,
  val filename: String,
  val header: Option[String],
  val compressedOutput: Boolean,
  val skipFirstInputLine: Boolean,
  val linkOutputs: Boolean,
  val suffixLength: Option[Parameter[Int]],
  val numberOfFiles: Option[Parameter[Int]],
  val linesPerFile: Option[Long],
  val bytesPerFile: Option[String],
  val bufferSize: Option[String],
  val pattern: Option[String],
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

  def copy(
    id: PipelineObjectId = id,
    scriptUri: Option[String] = scriptUri,
    jarUri: String = jarUri,
    mainClass: String = mainClass,
    filename: String = filename,
    header: Option[String] = header,
    compressedOutput: Boolean = compressedOutput,
    skipFirstInputLine: Boolean = skipFirstInputLine,
    linkOutputs: Boolean = linkOutputs,
    suffixLength: Option[Parameter[Int]] = suffixLength,
    numberOfFiles: Option[Parameter[Int]] = numberOfFiles,
    linesPerFile: Option[Long] = linesPerFile,
    bytesPerFile: Option[String] = bytesPerFile,
    bufferSize: Option[String] = bufferSize,
    pattern: Option[String] = pattern,
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
  ) = new SplitMergeFilesActivity(id,
    scriptUri, jarUri, mainClass,
    filename, header, compressedOutput, skipFirstInputLine, linkOutputs,
    suffixLength, numberOfFiles, linesPerFile, bytesPerFile, bufferSize,
    pattern, input, output, stdout, stderr, runsOn, dependsOn,
    preconditions, onFailAlarms, onSuccessAlarms, onLateActionAlarms,
    attemptTimeout, lateAfterTimeout, maximumRetries, retryDelay, failureAndRerunMode
  )

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withCompressedOutput() = this.copy(compressedOutput = true)
  def withSkipFirstInputLine() = this.copy(skipFirstInputLine = true)
  def withLinkOutputs() = this.copy(linkOutputs = true)
  def withHeader(header: String*) = this.copy(header = Option(header.mkString(",")))
  def withSuffixLength(suffixLength: Parameter[Int]) = this.copy(suffixLength = Option(suffixLength))
  def withNumberOfFiles(numberOfFiles: Parameter[Int]) = this.copy(numberOfFiles = Option(numberOfFiles))
  def withNumberOfLinesPerFile(linesPerFile: Long) = this.copy(linesPerFile = Option(linesPerFile))
  def withNumberOfBytesPerFile(bytesPerFile: String) = this.copy(bytesPerFile = Option(bytesPerFile))
  def withBufferSize(bufferSize: String) = this.copy(bufferSize = Option(bufferSize))
  def withInputPattern(pattern: String) = this.copy(pattern = Option(pattern))

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

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  private def arguments: Seq[String] = Seq(
    if (compressedOutput) Option(Seq("-z")) else None,
    if (skipFirstInputLine) Option(Seq("--skip-first-line")) else None,
    if (linkOutputs) Option(Seq("--link")) else None,
    header.map(h => Seq("--header", h)),
    suffixLength.map(s => Seq("--suffix-length", s.toString)),
    numberOfFiles.map(n => Seq("-n", n.toString)),
    linesPerFile.map(n => Seq("-l", n.toString)),
    bytesPerFile.map(n => Seq("-C", n)),
    bufferSize.map(n => Seq("-S", n)),
    pattern.map(p => Seq("--name", p)),
    Option(Seq(filename))
  ).flatten.flatten

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = scriptUri,
    scriptArgument = Option(Seq(jarUri, mainClass) ++ arguments),
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

object SplitMergeFilesActivity extends RunnableObject {

  def apply(filename: String)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): SplitMergeFilesActivity =
    new SplitMergeFilesActivity(
      id = PipelineObjectId(SplitMergeFilesActivity.getClass),
      scriptUri = Option(s"${hc.scriptUri}activities/run-jar.sh"),
      jarUri = s"${hc.scriptUri}activities/hyperion-file-activity-current-assembly.jar",
      mainClass = "com.krux.hyperion.contrib.activity.file.RepartitionFile",
      filename = filename,
      header = None,
      compressedOutput = false,
      skipFirstInputLine = false,
      linkOutputs = false,
      suffixLength = None,
      numberOfFiles = None,
      linesPerFile = None,
      bytesPerFile = None,
      bufferSize = None,
      pattern = None,
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
