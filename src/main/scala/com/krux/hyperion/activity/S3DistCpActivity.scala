package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpEmrActivity
import com.krux.hyperion.common.{StorageClass, PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.EmrCluster

class S3DistCpActivity private (
  val id: PipelineObjectId,
  val runsOn: EmrCluster,
  val dependsOn: Seq[PipelineActivity] = Seq(),
  val preconditions: Seq[Precondition] = Seq(),
  val onFailAlarms: Seq[SnsAlarm] = Seq(),
  val onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  val onLateActionAlarms: Seq[SnsAlarm] = Seq(),
  val source: Option[S3DataNode] = None,
  val dest: Option[S3DataNode] = None,
  val sourcePattern: Option[String] = None,
  val groupBy: Option[String] = None,
  val targetSize: Option[Int] = None,
  val appendLastToFile: Boolean = false,
  val outputCodec: S3DistCpActivity.OutputCodec = S3DistCpActivity.OutputCodec.None,
  val s3ServerSideEncryption: Boolean = false,
  val deleteOnSuccess: Boolean = false,
  val disableMultipartUpload: Boolean = false,
  val chunkSize: Option[Int] = None,
  val numberFiles: Boolean = false,
  val startingIndex: Option[Int] = None,
  val outputManifest: Option[String] = None,
  val previousManifest: Option[String] = None,
  val requirePreviousManifest: Boolean = false,
  val copyFromManifest: Boolean = false,
  val endpoint: Option[String] = None,
  val storageClass: Option[StorageClass] = None,
  val sourcePrefixesFile: Option[String] = None
) extends EmrActivity {

  def copy(
    id: PipelineObjectId = id,
    runsOn: EmrCluster = runsOn,
    dependsOn: Seq[PipelineActivity] = dependsOn,
    preconditions: Seq[Precondition] = preconditions,
    onFailAlarms: Seq[SnsAlarm] = onFailAlarms,
    onSuccessAlarms: Seq[SnsAlarm] = onSuccessAlarms,
    onLateActionAlarms: Seq[SnsAlarm] = onLateActionAlarms,
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
    sourcePrefixesFile: Option[String] = sourcePrefixesFile
  ) = new S3DistCpActivity(id, runsOn, dependsOn, preconditions, onFailAlarms, onSuccessAlarms, onLateActionAlarms,
    source, dest, sourcePattern, groupBy, targetSize, appendLastToFile, outputCodec, s3ServerSideEncryption,
    deleteOnSuccess, disableMultipartUpload, chunkSize, numberFiles, startingIndex, outputManifest,
    previousManifest, requirePreviousManifest, copyFromManifest, endpoint, storageClass, sourcePrefixesFile)

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

  private[hyperion] def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

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

  private def steps: Seq[MapReduceStep] = Seq(
    MapReduceStep()
      .withJar("/home/hadoop/lib/emr-s3distcp-1.0.jar")
      .withArguments(arguments: _*)
  )

  lazy val serialize = AdpEmrActivity(
    id = id,
    name = id.toOption,
    input = None,
    output = None,
    preStepCommand = None,
    postStepCommand = None,
    actionOnResourceFailure = None,
    actionOnTaskFailure = None,
    step = steps.map(_.toString),
    runsOn = runsOn.ref,
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref)
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

  def apply(runsOn: EmrCluster) =
    new S3DistCpActivity(
      id = PipelineObjectId("S3DistCpActivity"),
      runsOn = runsOn
    )

}
