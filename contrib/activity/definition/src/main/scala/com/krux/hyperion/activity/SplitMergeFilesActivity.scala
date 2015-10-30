package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.{HInt, HDuration, HString, HBoolean, HType, HLong}
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

class SplitMergeFilesActivity private (
  val id: PipelineObjectId,
  val scriptUri: Option[HString],
  val jarUri: HString,
  val mainClass: HString,
  val filename: HString,
  val header: Option[HString],
  val compressedOutput: HBoolean,
  val skipFirstInputLine: HBoolean,
  val linkOutputs: HBoolean,
  val suffixLength: Option[HInt],
  val numberOfFiles: Option[HInt],
  val linesPerFile: Option[HLong],
  val bytesPerFile: Option[HString],
  val bufferSize: Option[HString],
  val pattern: Option[HString],
  val markSuccessfulJobs: HBoolean,
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

  def copy(
    id: PipelineObjectId = id,
    scriptUri: Option[HString] = scriptUri,
    jarUri: HString = jarUri,
    mainClass: HString = mainClass,
    filename: HString = filename,
    header: Option[HString] = header,
    compressedOutput: HBoolean = compressedOutput,
    skipFirstInputLine: HBoolean = skipFirstInputLine,
    linkOutputs: HBoolean = linkOutputs,
    suffixLength: Option[HInt] = suffixLength,
    numberOfFiles: Option[HInt] = numberOfFiles,
    linesPerFile: Option[HLong] = linesPerFile,
    bytesPerFile: Option[HString] = bytesPerFile,
    bufferSize: Option[HString] = bufferSize,
    pattern: Option[HString] = pattern,
    markSuccessfulJobs: HBoolean = markSuccessfulJobs,
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
  ) = new SplitMergeFilesActivity(id,
    scriptUri, jarUri, mainClass,
    filename, header, compressedOutput, skipFirstInputLine, linkOutputs,
    suffixLength, numberOfFiles, linesPerFile, bytesPerFile, bufferSize,
    pattern, markSuccessfulJobs, input, output, stdout, stderr, runsOn, dependsOn,
    preconditions, onFailAlarms, onSuccessAlarms, onLateActionAlarms,
    attemptTimeout, lateAfterTimeout, maximumRetries, retryDelay, failureAndRerunMode
  )

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withCompressedOutput() = this.copy(compressedOutput = true)
  def withSkipFirstInputLine() = this.copy(skipFirstInputLine = true)
  def withLinkOutputs() = this.copy(linkOutputs = true)
  def withHeader(header: HString*) = this.copy(header = Option(header.mkString(","): HString))
  def withSuffixLength(suffixLength: HInt) = this.copy(suffixLength = Option(suffixLength))
  def withNumberOfFiles(numberOfFiles: HInt) = this.copy(numberOfFiles = Option(numberOfFiles))
  def withNumberOfLinesPerFile(linesPerFile: HLong) = this.copy(linesPerFile = Option(linesPerFile))
  def withNumberOfBytesPerFile(bytesPerFile: HString) = this.copy(bytesPerFile = Option(bytesPerFile))
  def withBufferSize(bufferSize: HString) = this.copy(bufferSize = Option(bufferSize))
  def withInputPattern(pattern: HString) = this.copy(pattern = Option(pattern))
  def markingSuccessfulJobs() = this.copy(markSuccessfulJobs = true)

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

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  private def arguments: Seq[HType] = Seq(
    if (compressedOutput) Option(Seq[HString]("-z")) else None,
    if (skipFirstInputLine) Option(Seq[HString]("--skip-first-line")) else None,
    if (linkOutputs) Option(Seq[HString]("--link")) else None,
    if (markSuccessfulJobs) Option(Seq[HString]("--mark-successful-jobs")) else None,
    header.map(h => Seq[HString]("--header", h)),
    suffixLength.map(s => Seq[HType]("--suffix-length", s)),
    numberOfFiles.map(n => Seq[HType]("-n", n)),
    linesPerFile.map(n => Seq[HType]("-l", n)),
    bytesPerFile.map(n => Seq[HString]("-C", n)),
    bufferSize.map(n => Seq[HString]("-S", n)),
    pattern.map(p => Seq[HString]("--name", p)),
    Option(Seq[HString](filename))
  ).flatten.flatten

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = scriptUri.map(_.serialize),
    scriptArgument = Option((jarUri +: mainClass +: arguments).map(_.serialize)),
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

object SplitMergeFilesActivity extends RunnableObject {

  def apply(filename: String)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): SplitMergeFilesActivity =
    new SplitMergeFilesActivity(
      id = PipelineObjectId(SplitMergeFilesActivity.getClass),
      scriptUri = Option(s"${hc.scriptUri}activities/run-jar.sh": HString),
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
      markSuccessfulJobs = false,
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
