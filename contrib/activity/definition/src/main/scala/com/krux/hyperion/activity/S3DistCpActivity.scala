package com.krux.hyperion.activity

import com.krux.hyperion.adt.{ HBoolean, HInt, HString }
import com.krux.hyperion.aws.AdpEmrActivity
import com.krux.hyperion.common.{ BaseFields, PipelineObjectId, StorageClass, S3Uri, HdfsUri }
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.resource._

case class S3DistCpActivityFields(
  source: Option[HString],
  dest: Option[HString],
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
  def withSource(source: HdfsUri) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(source = Option(source.ref: HString))
  )
  def withSource(source: S3Uri) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(source = Option(source.ref: HString))
  )
  def withSource(source: S3DataNode) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(source = Option(source.toString: HString))
  )

  def dest = s3DistCpActivityFields.dest
  def withDestination(dest: HdfsUri) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(dest = Option(dest.ref: HString))
  )
  def withDestination(dest: S3Uri) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(dest = Option(dest.ref: HString))
  )
  def withDestination(dest: S3DataNode) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(dest = Option(dest.toString: HString))
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
    s3DistCpActivityFields.copy(appendLastToFile = HBoolean.True)
  )

  def outputCodec = s3DistCpActivityFields.outputCodec
  def withOutputCodec(outputCodec: S3DistCpActivity.OutputCodec) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(outputCodec = outputCodec)
  )

  def s3ServerSideEncryption = s3DistCpActivityFields.s3ServerSideEncryption
  def withS3ServerSideEncryption() = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(s3ServerSideEncryption = HBoolean.True)
  )

  def deleteOnSuccess = s3DistCpActivityFields.deleteOnSuccess
  def withDeleteOnSuccess() = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(deleteOnSuccess = HBoolean.True)
  )

  def disableMultipartUpload = s3DistCpActivityFields.disableMultipartUpload
  def withoutMultipartUpload() = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(disableMultipartUpload = HBoolean.True)
  )

  def chunkSize = s3DistCpActivityFields.chunkSize
  def withMultipartUploadChunkSize(chunkSize: HInt) = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(chunkSize = Option(chunkSize))
  )

  def numberFiles = s3DistCpActivityFields.numberFiles
  def withNumberFiles() = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(numberFiles = HBoolean.True)
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
    s3DistCpActivityFields.copy(requirePreviousManifest = HBoolean.True)
  )

  def copyFromManifest = s3DistCpActivityFields.copyFromManifest
  def withCopyFromManifest() = updateS3DistCpActivityFields(
    s3DistCpActivityFields.copy(copyFromManifest = HBoolean.True)
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

  private def allArguments: Seq[HString] = Seq(
    Option(arguments),
    source.map(s => Seq[HString]("--src", s.serialize)),
    dest.map(s => Seq[HString]("--dest", s.serialize)),
    sourcePattern.map(s => Seq[HString]("--srcPattern", s.toString)),
    groupBy.map(s => Seq[HString]("--groupBy", s.toString)),
    targetSize.map(s => Seq[HString]("--targetSize", s.toString)),
    appendLastToFile.exists(Seq[HString]("--appendToLastFile")),
    Option(Seq[HString]("--outputCodec", outputCodec.toString)),
    s3ServerSideEncryption.exists(Seq[HString]("--s3ServerSideEncryption")),
    deleteOnSuccess.exists(Seq[HString]("--deleteOnSuccess")),
    disableMultipartUpload.exists(Seq[HString]("--disableMultipartUpload")),
    chunkSize.map(s => Seq[HString]("--multipartUploadChunkSize", s.toString)),
    numberFiles.exists(Seq[HString]("--numberFiles")),
    startingIndex.map(s => Seq[HString]("--startingIndex", s.toString)),
    outputManifest.map(s => Seq[HString]("--outputManifest", s)),
    previousManifest.map(s => Seq[HString]("--previousManifest", s)),
    requirePreviousManifest.exists(Seq[HString]("--requirePreviousManifest")),
    copyFromManifest.exists(Seq[HString]("--copyFromManifest")),
    endpoint.map(s => Seq[HString]("--endpoint", s)),
    storageClass.map(s => Seq[HString]("--storageClass", s.toString)),
    sourcePrefixesFile.map(s => Seq[HString]("--srcPrefixesFile", s))
  ).flatten.flatten

  private def steps: Seq[MapReduceStep] = Seq(
    if (runsOn.asManagedResource.flatMap(_.releaseLabel).nonEmpty)
      MapReduceStep("command-runner.jar")
        .withArguments((("s3-dist-cp": HString) +: allArguments): _*)
    else
      MapReduceStep("/home/hadoop/lib/emr-s3distcp-1.0.jar")
        .withArguments(allArguments: _*)
  )

  lazy val serialize = AdpEmrActivity(
    id = id,
    name = name,
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
    maxActiveInstances = maxActiveInstances.map(_.serialize)
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
        appendLastToFile = HBoolean.False,
        outputCodec = S3DistCpActivity.OutputCodec.None,
        s3ServerSideEncryption = HBoolean.False,
        deleteOnSuccess = HBoolean.False,
        disableMultipartUpload = HBoolean.False,
        chunkSize = None,
        numberFiles = HBoolean.False,
        startingIndex = None,
        outputManifest = None,
        previousManifest = None,
        requirePreviousManifest = HBoolean.False,
        copyFromManifest = HBoolean.False,
        endpoint = None,
        storageClass = None,
        sourcePrefixesFile = None
      ),
      preStepCommands = Seq.empty,
      postStepCommands = Seq.empty,
      arguments = Seq.empty
    )

}
