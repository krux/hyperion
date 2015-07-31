package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{WorkerGroup, Ec2Resource}

class SplitMergeFilesActivity private (
  val id: PipelineObjectId,
  val scriptUri: Option[String],
  val jar: String,
  val mainClass: String,
  val filename: String,
  val header: Option[String],
  val compressedOutput: Boolean,
  val skipFirstInputLine: Boolean,
  val linkOutputs: Boolean,
  val suffixLength: Option[Int],
  val numberOfFiles: Option[Int],
  val linesPerFile: Option[Long],
  val bytesPerFile: Option[Long],
  val bufferSize: Option[Long],
  val pattern: Option[String],
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

  def copy(
    id: PipelineObjectId = id,
    scriptUri: Option[String] = scriptUri,
    jar: String = jar,
    mainClass: String = mainClass,
    filename: String = filename,
    header: Option[String] = header,
    compressedOutput: Boolean = compressedOutput,
    skipFirstInputLine: Boolean = skipFirstInputLine,
    linkOutputs: Boolean = linkOutputs,
    suffixLength: Option[Int] = suffixLength,
    numberOfFiles: Option[Int] = numberOfFiles,
    linesPerFile: Option[Long] = linesPerFile,
    bytesPerFile: Option[Long] = bytesPerFile,
    bufferSize: Option[Long] = bufferSize,
    pattern: Option[String] = pattern,
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
  ) = new SplitMergeFilesActivity(id,
    scriptUri, jar, mainClass,
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
  def withSuffixLength(suffixLength: Int) = this.copy(suffixLength = Option(suffixLength))
  def withNumberOfFiles(numberOfFiles: Int) = this.copy(numberOfFiles = Option(numberOfFiles))
  def withNumberOfLinesPerFile(linesPerFile: Long) = this.copy(linesPerFile = Option(linesPerFile))
  def withNumberOfBytesPerFile(bytesPerFile: Long) = this.copy(bytesPerFile = Option(bytesPerFile))
  def withBufferSize(bufferSize: Long) = this.copy(bufferSize = Option(bufferSize))
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
  def withAttemptTimeout(timeout: String) = this.copy(attemptTimeout = Option(timeout))
  def withLateAfterTimeout(timeout: String) = this.copy(lateAfterTimeout = Option(timeout))
  def withMaximumRetries(retries: Int) = this.copy(maximumRetries = Option(retries))
  def withRetryDelay(delay: String) = this.copy(retryDelay = Option(delay))
  def withFailureAndRerunMode(mode: FailureAndRerunMode) = this.copy(failureAndRerunMode = Option(mode))

  override def objects: Iterable[PipelineObject] = runsOn.left.toSeq ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  private def arguments: Seq[String] = Seq(
    if (compressedOutput) Option(Seq("-z")) else None,
    if (skipFirstInputLine) Option(Seq("--skip-first-line")) else None,
    if (linkOutputs) Option(Seq("--link")) else None,
    header.map(h => Seq("--header", h)),
    suffixLength.map(s => Seq("--suffix-length", s.toString)),
    numberOfFiles.map(n => Seq("-n", n.toString)),
    linesPerFile.map(n => Seq("-l", n.toString)),
    bytesPerFile.map(n => Seq("-C", n.toString)),
    bufferSize.map(n => Seq("-S", n.toString)),
    pattern.map(p => Seq("--name", p)),
    Option(Seq(filename))
  ).flatten.flatten

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = scriptUri,
    scriptArgument = Option(Seq(jar, mainClass) ++ arguments),
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

object SplitMergeFilesActivity {

  def apply(filename: String, runsOn: Ec2Resource)(implicit hc: HyperionContext): SplitMergeFilesActivity = apply(filename, Left(runsOn))

  def apply(filename: String, runsOn: WorkerGroup)(implicit hc: HyperionContext): SplitMergeFilesActivity = apply(filename, Right(runsOn))

  private def apply(filename: String, runsOn: Either[Ec2Resource, WorkerGroup])(implicit hc: HyperionContext): SplitMergeFilesActivity =
    new SplitMergeFilesActivity(
      id = PipelineObjectId(SplitMergeFilesActivity.getClass),
      scriptUri = Option(s"${hc.scriptUri}activities/run-jar.sh"),
      jar = s"${hc.scriptUri}activities/hyperion-contrib-activity-file-assembly-current.jar",
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