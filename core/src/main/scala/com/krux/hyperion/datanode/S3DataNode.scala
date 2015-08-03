package com.krux.hyperion.datanode

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.{AdpSnsAlarm, AdpRef, AdpS3DataNode}
import com.krux.hyperion.common.{S3Uri, PipelineObjectId, PipelineObject}
import com.krux.hyperion.dataformat.DataFormat
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.WorkerGroup

sealed trait S3DataNode extends Copyable {

  def asInput(): String = asInput(1)
  def asInput(n: Integer): String = "${" + s"INPUT${n}_STAGING_DIR}"

  def asOutput(): String = asOutput(1)
  def asOutput(n: Integer): String = "${" + s"OUTPUT${n}_STAGING_DIR}"

  def withDataFormat(fmt: DataFormat): S3DataNode

  def named(name: String): S3DataNode
  def groupedBy(client: String): S3DataNode

}

object S3DataNode {

  def apply(s3Path: S3Uri): S3DataNode = apply(s3Path, None)

  def apply(s3Path: S3Uri, workerGroup: WorkerGroup): S3DataNode = apply(s3Path, Option(workerGroup))

  private def apply(s3Path: S3Uri, runsOn: Option[WorkerGroup]): S3DataNode =
    if (s3Path.ref.endsWith("/")) S3Folder(s3Path, runsOn)
    else S3File(s3Path, runsOn)
}

/**
 * Defines data from s3
 */
case class S3File private (
  id: PipelineObjectId,
  filePath: S3Uri,
  dataFormat: Option[DataFormat],
  manifestFilePath: Option[S3Uri],
  isCompressed: Boolean,
  isEncrypted: Boolean,
  workerGroup: Option[WorkerGroup],
  preconditions: Seq[Precondition],
  onSuccessAlarms: Seq[SnsAlarm],
  onFailAlarms: Seq[SnsAlarm]
) extends S3DataNode {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withDataFormat(fmt: DataFormat) = this.copy(dataFormat = Option(fmt))
  def withManifestFilePath(path: S3Uri) = this.copy(manifestFilePath = Option(path))
  def compressed = this.copy(isCompressed = true)
  def unencrypted = this.copy(isEncrypted = false)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)

  override def toString: String = filePath.toString

  def objects: Iterable[PipelineObject] = dataFormat

  lazy val serialize = AdpS3DataNode(
    id = id,
    name = id.toOption,
    directoryPath = None,
    filePath = Option(filePath.ref),
    dataFormat = dataFormat.map(_.ref),
    manifestFilePath = manifestFilePath.map(_.ref),
    compression = if (isCompressed) Option("gzip") else None,
    s3EncryptionType = if (isEncrypted) None else Option("NONE"),
    workerGroup = workerGroup.map(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref)
  )

}

object S3File {
  def apply(filePath: S3Uri): S3File = apply(filePath, None)

  def apply(filePath: S3Uri, workerGroup: WorkerGroup): S3File = apply(filePath, Option(workerGroup))

  private[datanode] def apply(filePath: S3Uri, runsOn: Option[WorkerGroup]): S3File =
    new S3File(
      id = PipelineObjectId(S3File.getClass),
      filePath = filePath,
      dataFormat = None,
      manifestFilePath = None,
      isCompressed = false,
      isEncrypted = true,
      workerGroup = runsOn,
      preconditions = Seq(),
      onSuccessAlarms = Seq(),
      onFailAlarms = Seq()
    )
}

/**
 * Defines data from s3 directory
 */
case class S3Folder private(
  id: PipelineObjectId,
  directoryPath: S3Uri,
  dataFormat: Option[DataFormat],
  manifestFilePath: Option[S3Uri],
  isCompressed: Boolean,
  isEncrypted: Boolean,
  workerGroup: Option[WorkerGroup],
  preconditions: Seq[Precondition],
  onSuccessAlarms: Seq[SnsAlarm],
  onFailAlarms: Seq[SnsAlarm]
) extends S3DataNode {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withDataFormat(fmt: DataFormat) = this.copy(dataFormat = Option(fmt))
  def withManifestFilePath(path: S3Uri) = this.copy(manifestFilePath = Option(path))
  def compressed = this.copy(isCompressed = true)
  def unencrypted = this.copy(isEncrypted = false)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions ++ preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)

  override def toString: String = directoryPath.toString

  def objects: Iterable[PipelineObject] = dataFormat

  lazy val serialize = AdpS3DataNode(
    id = id,
    name = id.toOption,
    directoryPath = Option(directoryPath.ref),
    filePath = None,
    dataFormat = dataFormat.map(_.ref),
    manifestFilePath = manifestFilePath.map(_.ref),
    compression = if (isCompressed) Option("gzip") else None,
    s3EncryptionType = if (isEncrypted) None else Option("NONE"),
    workerGroup = workerGroup.map(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref)
  )
}

object S3Folder {
  def apply(directoryPath: S3Uri): S3Folder = apply(directoryPath, None)

  def apply(directoryPath: S3Uri, workerGroup: WorkerGroup): S3Folder = apply(directoryPath, Option(workerGroup))

  private[datanode] def apply(directoryPath: S3Uri, runsOn: Option[WorkerGroup]): S3Folder =
    new S3Folder(
      id = PipelineObjectId(S3Folder.getClass),
      directoryPath = directoryPath,
      dataFormat = None,
      manifestFilePath = None,
      isCompressed = false,
      isEncrypted = true,
      workerGroup = runsOn,
      preconditions = Seq(),
      onSuccessAlarms = Seq(),
      onFailAlarms = Seq()
    )
}
