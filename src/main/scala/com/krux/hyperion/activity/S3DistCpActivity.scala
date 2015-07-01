package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpEmrActivity
import com.krux.hyperion.common.{StorageClass, PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.EmrCluster

case class S3DistCpActivity private (
  id: PipelineObjectId,
  runsOn: EmrCluster,
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm],
  source: Option[S3DataNode] = None,
  dest: Option[S3DataNode] = None,
  sourcePattern: Option[String] = None,
  groupBy: Option[String] = None,
  targetSize: Option[Int] = None,
  appendLastToFile: Boolean = false,
  outputCodec: S3DistCpActivity.OutputCodec = S3DistCpActivity.OutputCodec.None,
  s3ServerSideEncryption: Boolean = false,
  deleteOnSuccess: Boolean = false,
  disableMultipartUpload: Boolean = false,
  chunkSize: Option[Int] = None,
  numberFiles: Boolean = false,
  startingIndex: Option[Int] = None,
  outputManifest: Option[String] = None,
  previousManifest: Option[String] = None,
  requirePreviousManifest: Boolean = false,
  copyFromManifest: Boolean = false,
  endpoint: Option[String] = None,
  storageClass: Option[StorageClass] = None,
  sourcePrefixesFile: Option[String] = None
) extends EmrActivity {

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

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
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
    step = steps.map(_.toStepString),
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
      runsOn = runsOn,
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )

}
