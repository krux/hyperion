package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpEmrActivity
import com.krux.hyperion.common.{StorageClass, PipelineObject, PipelineObjectId, BaseFields}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.adt.{HInt, HDuration, HString, HBoolean}
import com.krux.hyperion.adt.HType._
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource._

case class S3DistCpActivityFields(
  source: Option[S3DataNode],
  dest: Option[S3DataNode],
  sourcePattern: Option[HString],
  groupBy: Option[HString],
  targetSize: Option[HInt],
  appendLastToFile: HBoolean,
  outputCodec: S3DistCpActivity.OutputCodec,
  s3ServerSideEncryption: HBoolean,
  deleteOnSuccess: HBoolean,
  disableMultipartUpload: HBoolean,
  chunkSize: Option[HInt],
  numberFiles: HBoolean,
  startingIndex: Option[HInt],
  outputManifest: Option[HString],
  previousManifest: Option[HString],
  requirePreviousManifest: HBoolean,
  copyFromManifest: HBoolean,
  endpoint: Option[HString],
  storageClass: Option[StorageClass],
  sourcePrefixesFile: Option[HString]
)

case class S3DistCpActivity[A <: EmrCluster] private (
  baseFields: BaseFields,
  activityFields: ActivityFields[A],
  s3DistCpActivityFields: S3DistCpActivityFields,
  preStepCommands: Seq[HString],
  postStepCommands: Seq[HString],
  arguments: Seq[HString]
) extends EmrActivity[A] {

  type Self = S3DistCpActivity[A]

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[A]) = copy(activityFields = fields)
  def updateS3DistCpActivityFields(fields: S3DistCpActivityFields) = copy(s3DistCpActivityFields = fields)

  def source = s3DistCpActivityFields.source
  def withSource(source: S3DataNode) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(source = Option(source))
  )

  def dest = s3DistCpActivityFields.dest
  def withDestination(dest: S3DataNode) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(dest = Option(dest))
  )

  def sourcePattern = s3DistCpActivityFields.sourcePattern
  def withSourcePattern(sourcePattern: HString) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(sourcePattern = Option(sourcePattern))
  )

  def groupBy = s3DistCpActivityFields.groupBy
  def withGroupBy(groupBy: HString) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(groupBy = Option(groupBy))
  )

  def targetSize = s3DistCpActivityFields.targetSize
  def withTargetSize(targetSize: HInt) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(targetSize = Option(targetSize))
  )

  def appendLastToFile = s3DistCpActivityFields.appendLastToFile
  def appendToLastFile() = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(appendLastToFile = true)
  )

  def outputCodec = s3DistCpActivityFields.outputCodec
  def withOutputCodec(outputCodec: S3DistCpActivity.OutputCodec) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(outputCodec = outputCodec)
  )

  def s3ServerSideEncryption = s3DistCpActivityFields.s3ServerSideEncryption
  def withS3ServerSideEncryption() = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(s3ServerSideEncryption = true)
  )

  def deleteOnSuccess = s3DistCpActivityFields.deleteOnSuccess
  def withDeleteOnSuccess() = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(deleteOnSuccess = true)
  )

  def disableMultipartUpload = s3DistCpActivityFields.disableMultipartUpload
  def withoutMultipartUpload() = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(disableMultipartUpload = true)
  )

  def chunkSize = s3DistCpActivityFields.chunkSize
  def withMultipartUploadChunkSize(chunkSize: HInt) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(chunkSize = Option(chunkSize))
  )

  def numberFiles = s3DistCpActivityFields.numberFiles
  def withNumberFiles() = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(numberFiles = true)
  )

  def startingIndex = s3DistCpActivityFields.startingIndex
  def withStartingIndex(startingIndex: HInt) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(startingIndex = Option(startingIndex))
  )

  def outputManifest = s3DistCpActivityFields.outputManifest
  def withOutputManifest(outputManifest: HString) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(outputManifest = Option(outputManifest))
  )

  def previousManifest = s3DistCpActivityFields.previousManifest
  def withPreviousManifest(previousManifest: HString) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(previousManifest = Option(previousManifest))
  )

  def requirePreviousManifest = s3DistCpActivityFields.requirePreviousManifest
  def withRequirePreviousManifest() = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(requirePreviousManifest = true)
  )

  def copyFromManifest = s3DistCpActivityFields.copyFromManifest
  def withCopyFromManifest() = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(copyFromManifest = true)
  )

  def endpoint = s3DistCpActivityFields.endpoint
  def withS3Endpoint(endpoint: HString) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(endpoint = Option(endpoint))
  )

  def storageClass = s3DistCpActivityFields.storageClass
  def withStorageClass(storageClass: StorageClass) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(storageClass = Option(storageClass))
  )

  def sourcePrefixesFile = s3DistCpActivityFields.sourcePrefixesFile
  def withSourcePrefixesFile(sourcePrefixesFile: HString) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(sourcePrefixesFile = Option(sourcePrefixesFile))
  )

  def withArgument(argument: HString*) = copy(arguments = arguments ++ argument)

  override def objects = source ++ dest ++ super.objects

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
    failureAndRerunMode = failureAndRerunMode.map(_.serialize)
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

  def apply[A <: EmrCluster](runsOn: Resource[A]): S3DistCpActivity[A] =
    new S3DistCpActivity(
      baseFields = BaseFields(PipelineObjectId(S3DistCpActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      s3DistCpActivityFields = S3DistCpActivityFields(
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
        sourcePrefixesFile = None
      ),
      preStepCommands = Seq.empty,
      postStepCommands = Seq.empty,
      arguments = Seq.empty
    )

}
