package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpEmrActivity
import com.krux.hyperion.common.{StorageClass, PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.adt.{HInt, HDuration, HString, HBoolean}
import com.krux.hyperion.adt.HType._
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource._

class S3DistCpActivity private (
  val id: PipelineObjectId,
  val source: Option[S3DataNode],
  val dest: Option[S3DataNode],
  val sourcePattern: Option[HString],
  val groupBy: Option[HString],
  val targetSize: Option[HInt],
  val appendLastToFile: HBoolean,
  val outputCodec: S3DistCpActivity.OutputCodec,
  val s3ServerSideEncryption: HBoolean,
  val deleteOnSuccess: HBoolean,
  val disableMultipartUpload: HBoolean,
  val chunkSize: Option[HInt],
  val numberFiles: HBoolean,
  val startingIndex: Option[HInt],
  val outputManifest: Option[HString],
  val previousManifest: Option[HString],
  val requirePreviousManifest: HBoolean,
  val copyFromManifest: HBoolean,
  val endpoint: Option[HString],
  val storageClass: Option[StorageClass],
  val sourcePrefixesFile: Option[HString],
  val preStepCommands: Seq[HString],
  val postStepCommands: Seq[HString],
  val runsOn: Resource[EmrCluster],
  val dependsOn: Seq[PipelineActivity],
  val preconditions: Seq[Precondition],
  val onFailAlarms: Seq[SnsAlarm],
  val onSuccessAlarms: Seq[SnsAlarm],
  val onLateActionAlarms: Seq[SnsAlarm],
  val attemptTimeout: Option[HDuration],
  val lateAfterTimeout: Option[HDuration],
  val maximumRetries: Option[HInt],
  val retryDelay: Option[HDuration],
  val failureAndRerunMode: Option[FailureAndRerunMode],
  val actionOnResourceFailure: Option[ActionOnResourceFailure],
  val actionOnTaskFailure: Option[ActionOnTaskFailure],
  val arguments: Seq[HString]
) extends EmrActivity {

  def copy(
    id: PipelineObjectId = id,
    source: Option[S3DataNode] = source,
    dest: Option[S3DataNode] = dest,
    sourcePattern: Option[HString] = sourcePattern,
    groupBy: Option[HString] = groupBy,
    targetSize: Option[HInt] = targetSize,
    appendLastToFile: HBoolean = appendLastToFile,
    outputCodec: S3DistCpActivity.OutputCodec = outputCodec,
    s3ServerSideEncryption: HBoolean = s3ServerSideEncryption,
    deleteOnSuccess: HBoolean = deleteOnSuccess,
    disableMultipartUpload: HBoolean = disableMultipartUpload,
    chunkSize: Option[HInt] = chunkSize,
    numberFiles: HBoolean = numberFiles,
    startingIndex: Option[HInt] = startingIndex,
    outputManifest: Option[HString] = outputManifest,
    previousManifest: Option[HString] = previousManifest,
    requirePreviousManifest: HBoolean = requirePreviousManifest,
    copyFromManifest: HBoolean = copyFromManifest,
    endpoint: Option[HString] = endpoint,
    storageClass: Option[StorageClass] = storageClass,
    sourcePrefixesFile: Option[HString] = sourcePrefixesFile,
    preStepCommands: Seq[HString] = preStepCommands,
    postStepCommands: Seq[HString] = postStepCommands,
    runsOn: Resource[EmrCluster] = runsOn,
    dependsOn: Seq[PipelineActivity] = dependsOn,
    preconditions: Seq[Precondition] = preconditions,
    onFailAlarms: Seq[SnsAlarm] = onFailAlarms,
    onSuccessAlarms: Seq[SnsAlarm] = onSuccessAlarms,
    onLateActionAlarms: Seq[SnsAlarm] = onLateActionAlarms,
    attemptTimeout: Option[HDuration] = attemptTimeout,
    lateAfterTimeout: Option[HDuration] = lateAfterTimeout,
    maximumRetries: Option[HInt] = maximumRetries,
    retryDelay: Option[HDuration] = retryDelay,
    failureAndRerunMode: Option[FailureAndRerunMode] = failureAndRerunMode,
    actionOnResourceFailure: Option[ActionOnResourceFailure] = actionOnResourceFailure,
    actionOnTaskFailure: Option[ActionOnTaskFailure] = actionOnTaskFailure,
    arguments: Seq[HString] = arguments
  ) = new S3DistCpActivity(id, source, dest, sourcePattern, groupBy, targetSize, appendLastToFile,
    outputCodec, s3ServerSideEncryption, deleteOnSuccess, disableMultipartUpload, chunkSize, numberFiles,
    startingIndex, outputManifest, previousManifest, requirePreviousManifest, copyFromManifest, endpoint,
    storageClass, sourcePrefixesFile, preStepCommands, postStepCommands, runsOn, dependsOn, preconditions,
    onFailAlarms, onSuccessAlarms, onLateActionAlarms,
    attemptTimeout, lateAfterTimeout, maximumRetries, retryDelay, failureAndRerunMode,
    actionOnResourceFailure, actionOnTaskFailure, arguments)

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withSource(source: S3DataNode) = this.copy(source = Option(source))
  def withDestination(dest: S3DataNode) = this.copy(dest = Option(dest))
  def withSourcePattern(sourcePattern: HString) = this.copy(sourcePattern = Option(sourcePattern))
  def withGroupBy(groupBy: HString) = this.copy(groupBy = Option(groupBy))
  def withTargetSize(targetSize: HInt) = this.copy(targetSize = Option(targetSize))
  def appendToLastFile() = this.copy(appendLastToFile = true)
  def withOutputCodec(outputCodec: S3DistCpActivity.OutputCodec) = this.copy(outputCodec = outputCodec)
  def withS3ServerSideEncryption() = this.copy(s3ServerSideEncryption = true)
  def withDeleteOnSuccess() = this.copy(deleteOnSuccess = true)
  def withoutMultipartUpload() = this.copy(disableMultipartUpload = true)
  def withMultipartUploadChunkSize(chunkSize: HInt) = this.copy(chunkSize = Option(chunkSize))
  def withNumberFiles() = this.copy(numberFiles = true)
  def withStartingIndex(startingIndex: HInt) = this.copy(startingIndex = Option(startingIndex))
  def withOutputManifest(outputManifest: HString) = this.copy(outputManifest = Option(outputManifest))
  def withPreviousManifest(previousManifest: HString) = this.copy(previousManifest = Option(previousManifest))
  def withRequirePreviousManifest() = this.copy(requirePreviousManifest = true)
  def withCopyFromManifest() = this.copy(copyFromManifest = true)
  def withS3Endpoint(endpoint: HString) = this.copy(endpoint = Option(endpoint))
  def withStorageClass(storageClass: StorageClass) = this.copy(storageClass = Option(storageClass))
  def withSourcePrefixesFile(sourcePrefixesFile: HString) = this.copy(sourcePrefixesFile = Option(sourcePrefixesFile))
  def withPreStepCommand(command: HString*) = this.copy(preStepCommands = preStepCommands ++ command)
  def withPostStepCommand(command: HString*) = this.copy(postStepCommands = postStepCommands ++ command)

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
  def withActionOnResourceFailure(action: ActionOnResourceFailure) = this.copy(actionOnResourceFailure = Option(action))
  def withActionOnTaskFailure(action: ActionOnTaskFailure) = this.copy(actionOnTaskFailure = Option(action))
  def withArgument(argument: HString*) = this.copy(arguments = arguments ++ argument)

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  private def allArguments: Seq[HString] = Seq(
    Option(arguments),
    source.map(s => Seq[HString]("--src", s.toString)),
    dest.map(s => Seq[HString]("--dest", s.toString)),
    sourcePattern.map(s => Seq[HString]("--srcPattern", s.toString)),
    groupBy.map(s => Seq[HString]("--groupBy", s.toString)),
    targetSize.map(s => Seq[HString]("--targetSize", s.toString)),
    if (appendLastToFile) Option(Seq[HString]("--appendToLastFile")) else None,
    Option(Seq[HString]("--outputCodec", outputCodec.toString)),
    if (s3ServerSideEncryption) Option(Seq[HString]("--s3ServerSideEncryption")) else None,
    if (deleteOnSuccess) Option(Seq[HString]("--deleteOnSuccess")) else None,
    if (disableMultipartUpload) Option(Seq[HString]("--disableMultipartUpload")) else None,
    chunkSize.map(s => Seq[HString]("--multipartUploadChunkSize", s.toString)),
    if (numberFiles) Option(Seq[HString]("--numberFiles")) else None,
    startingIndex.map(s => Seq[HString]("--startingIndex", s.toString)),
    outputManifest.map(s => Seq[HString]("--outputManifest", s)),
    previousManifest.map(s => Seq[HString]("--previousManifest", s)),
    if (requirePreviousManifest) Option(Seq[HString]("--requirePreviousManifest")) else None,
    if (copyFromManifest) Option(Seq[HString]("--copyFromManifest")) else None,
    endpoint.map(s => Seq[HString]("--endpoint", s)),
    storageClass.map(s => Seq[HString]("--storageClass", s.toString)),
    sourcePrefixesFile.map(s => Seq[HString]("--srcPrefixesFile", s))
  ).flatten.flatten

  private def steps: Seq[MapReduceStep] = Seq(MapReduceStep("/home/hadoop/lib/emr-s3distcp-1.0.jar").withArguments(allArguments: _*))

  lazy val serialize = AdpEmrActivity(
    id = id,
    name = id.toOption,
    step = steps.map(_.serialize),
    preStepCommand = seqToOption(preStepCommands)(_.serialize),
    postStepCommand = seqToOption(postStepCommands)(_.serialize),
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
    failureAndRerunMode = failureAndRerunMode.map(_.serialize),
    actionOnResourceFailure = actionOnResourceFailure.map(_.serialize),
    actionOnTaskFailure = actionOnTaskFailure.map(_.serialize)
  )

}

object S3DistCpActivity extends RunnableObject {

  sealed trait OutputCodec

  object OutputCodec {
    object Gz extends OutputCodec {
      override def toString = "gz"
    }

    object Gzip extends OutputCodec {
      override def toString = "gzip"
    }

    object Lzo extends OutputCodec {
      override def toString = "lzo"
    }

    object Snappy extends OutputCodec {
      override def toString = "snappy"
    }

    object None extends OutputCodec {
      override def toString = "none"
    }
  }

  def apply(runsOn: Resource[EmrCluster]): S3DistCpActivity =
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
      preStepCommands = Seq.empty,
      postStepCommands = Seq.empty,
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
      failureAndRerunMode = None,
      actionOnResourceFailure = None,
      actionOnTaskFailure = None,
      arguments = Seq.empty
    )

}
