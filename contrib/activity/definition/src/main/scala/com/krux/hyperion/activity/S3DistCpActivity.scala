package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpEmrActivity
import com.krux.hyperion.common.{StorageClass, PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{ActionOnTaskFailure, ActionOnResourceFailure, WorkerGroup, EmrCluster}

class S3DistCpActivity private (
  val id: PipelineObjectId,
  val source: Option[S3DataNode],
  val dest: Option[S3DataNode],
  val sourcePattern: Option[String],
  val groupBy: Option[String],
  val targetSize: Option[Int],
  val appendLastToFile: Boolean,
  val outputCodec: S3DistCpActivity.OutputCodec,
  val s3ServerSideEncryption: Boolean,
  val deleteOnSuccess: Boolean,
  val disableMultipartUpload: Boolean,
  val chunkSize: Option[Int],
  val numberFiles: Boolean,
  val startingIndex: Option[Int],
  val outputManifest: Option[String],
  val previousManifest: Option[String],
  val requirePreviousManifest: Boolean,
  val copyFromManifest: Boolean,
  val endpoint: Option[String],
  val storageClass: Option[StorageClass],
  val sourcePrefixesFile: Option[String],
  val preStepCommands: Seq[String],
  val postStepCommands: Seq[String],
  val runsOn: Either[EmrCluster, WorkerGroup],
  val dependsOn: Seq[PipelineActivity],
  val preconditions: Seq[Precondition],
  val onFailAlarms: Seq[SnsAlarm],
  val onSuccessAlarms: Seq[SnsAlarm],
  val onLateActionAlarms: Seq[SnsAlarm],
  val attemptTimeout: Option[String],
  val lateAfterTimeout: Option[String],
  val maximumRetries: Option[Int],
  val retryDelay: Option[String],
  val failureAndRerunMode: Option[FailureAndRerunMode],
  val actionOnResourceFailure: Option[ActionOnResourceFailure],
  val actionOnTaskFailure: Option[ActionOnTaskFailure]
) extends EmrActivity {

  def copy(
    id: PipelineObjectId = id,
    source: Option[S3DataNode] = source,
    dest: Option[S3DataNode] = dest,
    sourcePattern: Option[String] = sourcePattern,
    groupBy: Option[String] = groupBy,
    targetSize: Option[Int] = targetSize,
    appendLastToFile: Boolean = appendLastToFile,
    outputCodec: S3DistCpActivity.OutputCodec = outputCodec,
    s3ServerSideEncryption: Boolean = s3ServerSideEncryption,
    deleteOnSuccess: Boolean = deleteOnSuccess,
    disableMultipartUpload: Boolean = disableMultipartUpload,
    chunkSize: Option[Int] = chunkSize,
    numberFiles: Boolean = numberFiles,
    startingIndex: Option[Int] = startingIndex,
    outputManifest: Option[String] = outputManifest,
    previousManifest: Option[String] = previousManifest,
    requirePreviousManifest: Boolean = requirePreviousManifest,
    copyFromManifest: Boolean = copyFromManifest,
    endpoint: Option[String] = endpoint,
    storageClass: Option[StorageClass] = storageClass,
    sourcePrefixesFile: Option[String] = sourcePrefixesFile,
    preStepCommands: Seq[String] = preStepCommands,
    postStepCommands: Seq[String] = postStepCommands,
    runsOn: Either[EmrCluster, WorkerGroup] = runsOn,
    dependsOn: Seq[PipelineActivity] = dependsOn,
    preconditions: Seq[Precondition] = preconditions,
    onFailAlarms: Seq[SnsAlarm] = onFailAlarms,
    onSuccessAlarms: Seq[SnsAlarm] = onSuccessAlarms,
    onLateActionAlarms: Seq[SnsAlarm] = onLateActionAlarms,
    attemptTimeout: Option[String] = attemptTimeout,
    lateAfterTimeout: Option[String] = lateAfterTimeout,
    maximumRetries: Option[Int] = maximumRetries,
    retryDelay: Option[String] = retryDelay,
    failureAndRerunMode: Option[FailureAndRerunMode] = failureAndRerunMode,
    actionOnResourceFailure: Option[ActionOnResourceFailure] = actionOnResourceFailure,
    actionOnTaskFailure: Option[ActionOnTaskFailure] = actionOnTaskFailure
  ) = new S3DistCpActivity(id, source, dest, sourcePattern, groupBy, targetSize, appendLastToFile,
    outputCodec, s3ServerSideEncryption, deleteOnSuccess, disableMultipartUpload, chunkSize, numberFiles,
    startingIndex, outputManifest, previousManifest, requirePreviousManifest, copyFromManifest, endpoint,
    storageClass, sourcePrefixesFile, preStepCommands, postStepCommands, runsOn, dependsOn, preconditions,
    onFailAlarms, onSuccessAlarms, onLateActionAlarms,
    attemptTimeout, lateAfterTimeout, maximumRetries, retryDelay, failureAndRerunMode,
    actionOnResourceFailure, actionOnTaskFailure)

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withSource(source: S3DataNode) = this.copy(source = Option(source))
  def withDestination(dest: S3DataNode) = this.copy(dest = Option(dest))
  def withSourcePattern(sourcePattern: String) = this.copy(sourcePattern = Option(sourcePattern))
  def withGroupBy(groupBy: String) = this.copy(groupBy = Option(groupBy))
  def withTargetSize(targetSize: Int) = this.copy(targetSize = Option(targetSize))
  def appendToLastFile() = this.copy(appendLastToFile = true)
  def withOutputCodec(outputCodec: S3DistCpActivity.OutputCodec) = this.copy(outputCodec = outputCodec)
  def withS3ServerSideEncryption() = this.copy(s3ServerSideEncryption = true)
  def withDeleteOnSuccess() = this.copy(deleteOnSuccess = true)
  def withoutMultipartUpload() = this.copy(disableMultipartUpload = true)
  def withMultipartUploadChunkSize(chunkSize: Int) = this.copy(chunkSize = Option(chunkSize))
  def withNumberFiles() = this.copy(numberFiles = true)
  def withStartingIndex(startingIndex: Int) = this.copy(startingIndex = Option(startingIndex))
  def withOutputManifest(outputManifest: String) = this.copy(outputManifest = Option(outputManifest))
  def withPreviousManifest(previousManifest: String) = this.copy(previousManifest = Option(previousManifest))
  def withRequirePreviousManifest() = this.copy(requirePreviousManifest = true)
  def withCopyFromManifest() = this.copy(copyFromManifest = true)
  def withS3Endpoint(endpoint: String) = this.copy(endpoint = Option(endpoint))
  def withStorageClass(storageClass: StorageClass) = this.copy(storageClass = Option(storageClass))
  def withSourcePrefixesFile(sourcePrefixesFile: String) = this.copy(sourcePrefixesFile = Option(sourcePrefixesFile))
  def withPreStepCommand(command: String*) = this.copy(preStepCommands = preStepCommands ++ command)
  def withPostStepCommand(command: String*) = this.copy(postStepCommands = postStepCommands ++ command)

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
  def withActionOnResourceFailure(action: ActionOnResourceFailure) = this.copy(actionOnResourceFailure = Option(action))
  def withActionOnTaskFailure(action: ActionOnTaskFailure) = this.copy(actionOnTaskFailure = Option(action))

  override def objects: Iterable[PipelineObject] = runsOn.left.toSeq ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  private def arguments: Seq[String] = Seq(
    source.map(s => Seq("--src", s.toString)),
    dest.map(s => Seq("--dest", s.toString)),
    sourcePattern.map(s => Seq("--srcPattern", s.toString)),
    groupBy.map(s => Seq("--groupBy", s.toString)),
    targetSize.map(s => Seq("--targetSize", s.toString)),
    if (appendLastToFile) Option(Seq("--appendToLastFile")) else None,
    Option(Seq("--outputCodec", outputCodec.toString)),
    if (s3ServerSideEncryption) Option(Seq("--s3ServerSideEncryption")) else None,
    if (deleteOnSuccess) Option(Seq("--deleteOnSuccess")) else None,
    if (disableMultipartUpload) Option(Seq("--disableMultipartUpload")) else None,
    chunkSize.map(s => Seq("--multipartUploadChunkSize", s.toString)),
    if (numberFiles) Option(Seq("--numberFiles")) else None,
    startingIndex.map(s => Seq("--startingIndex", s.toString)),
    outputManifest.map(s => Seq("--outputManifest", s)),
    previousManifest.map(s => Seq("--previousManifest", s)),
    if (requirePreviousManifest) Option(Seq("--requirePreviousManifest")) else None,
    if (copyFromManifest) Option(Seq("--copyFromManifest")) else None,
    endpoint.map(s => Seq("--endpoint", s)),
    storageClass.map(s => Seq("--storageClass", s.toString)),
    sourcePrefixesFile.map(s => Seq("--srcPrefixesFile", s))
  ).flatten.flatten

  private def steps: Seq[MapReduceStep] = Seq(MapReduceStep("/home/hadoop/lib/emr-s3distcp-1.0.jar").withArguments(arguments: _*))

  lazy val serialize = AdpEmrActivity(
    id = id,
    name = id.toOption,
    step = steps.map(_.toString),
    preStepCommand = seqToOption(preStepCommands)(_.toString),
    postStepCommand = seqToOption(postStepCommands)(_.toString),
    input = None,
    output = None,
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
    failureAndRerunMode = failureAndRerunMode.map(_.toString),
    actionOnResourceFailure = actionOnResourceFailure.map(_.toString),
    actionOnTaskFailure = actionOnTaskFailure.map(_.toString)
  )

}

object S3DistCpActivity {

  trait OutputCodec

  object OutputCodec {
    object Gzip extends OutputCodec {
      override def toString: String = "gzip"
    }

    object Lzo extends OutputCodec {
      override def toString: String = "lzo"
    }

    object Snappy extends OutputCodec {
      override def toString: String = "snappy"
    }

    object None extends OutputCodec {
      override def toString: String = "none"
    }
  }

  def apply(runsOn: EmrCluster): S3DistCpActivity = apply(Left(runsOn))

  def apply(runsOn: WorkerGroup): S3DistCpActivity = apply(Right(runsOn))

  private def apply(runsOn: Either[EmrCluster, WorkerGroup]): S3DistCpActivity =
    new S3DistCpActivity(
      id = PipelineObjectId(S3DistCpActivity.getClass),
      source = None,
      dest = None,
      sourcePattern = None,
      groupBy = None,
      targetSize = None,
      appendLastToFile = false,
      outputCodec = S3DistCpActivity.OutputCodec.None,
      s3ServerSideEncryption = false,
      deleteOnSuccess = false,
      disableMultipartUpload = false,
      chunkSize = None,
      numberFiles = false,
      startingIndex = None,
      outputManifest = None,
      previousManifest = None,
      requirePreviousManifest = false,
      copyFromManifest = false,
      endpoint = None,
      storageClass = None,
      sourcePrefixesFile = None,
      preStepCommands = Seq(),
      postStepCommands = Seq(),
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
      failureAndRerunMode = None,
      actionOnResourceFailure = None,
      actionOnTaskFailure = None
    )

}