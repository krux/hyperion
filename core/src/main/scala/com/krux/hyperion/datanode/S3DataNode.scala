package com.krux.hyperion.datanode

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.{AdpSnsAlarm, AdpRef, AdpS3DataNode}
import com.krux.hyperion.common.{S3Uri, PipelineObjectId, PipelineObject}
import com.krux.hyperion.dataformat.DataFormat
import com.krux.hyperion.parameter.Parameter
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.WorkerGroup

sealed trait S3DataNode extends Copyable {

  def asInput(): String = asInput(1)
  def asInput(n: Integer): String = "${" + s"INPUT${n}_STAGING_DIR}"

  def asOutput(): String = asOutput(1)
  def asOutput(n: Integer): String = "${" + s"OUTPUT${n}_STAGING_DIR}"

  def named(name: String): S3DataNode
  def groupedBy(client: String): S3DataNode

  def withDataFormat(fmt: DataFormat): S3DataNode
  def withManifestFilePath(path: Parameter[S3Uri]): S3DataNode
  def compressed: S3DataNode
  def unencrypted: S3DataNode
  def whenMet(conditions: Precondition*): S3DataNode
  def onFail(alarms: SnsAlarm*): S3DataNode
  def onSuccess(alarms: SnsAlarm*): S3DataNode
}

object S3DataNode {

  def apply(s3Path: S3Uri): S3DataNode =
    if (s3Path.ref.endsWith("/")) S3Folder(s3Path)
    else S3File(s3Path)

}

/**
 * Defines data from s3
 */
case class S3File private (
  id: PipelineObjectId,
  filePath: Parameter[S3Uri],
  dataFormat: Option[DataFormat],
  manifestFilePath: Option[Parameter[S3Uri]],
  isCompressed: Boolean,
  isEncrypted: Boolean,
  preconditions: Seq[Precondition],
  onSuccessAlarms: Seq[SnsAlarm],
  onFailAlarms: Seq[SnsAlarm]
) extends S3DataNode {

  def named(name: String): S3File = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String): S3File = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withDataFormat(fmt: DataFormat): S3File = this.copy(dataFormat = Option(fmt))
  def withManifestFilePath(path: Parameter[S3Uri]): S3File = this.copy(manifestFilePath = Option(path))
  def compressed: S3File = this.copy(isCompressed = true)
  def unencrypted: S3File = this.copy(isEncrypted = false)
  def whenMet(conditions: Precondition*): S3File = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*): S3File = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*): S3File = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)

  override def toString = filePath.toString

  def objects: Iterable[PipelineObject] = dataFormat ++ preconditions ++ onSuccessAlarms ++ onFailAlarms

  lazy val serialize = AdpS3DataNode(
    id = id,
    name = id.toOption,
    directoryPath = None,
    filePath = Option(filePath.toString),
    dataFormat = dataFormat.map(_.ref),
    manifestFilePath = manifestFilePath.map(_.toString),
    compression = if (isCompressed) Option("gzip") else None,
    s3EncryptionType = if (isEncrypted) None else Option("NONE"),
    precondition = seqToOption(preconditions)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref)
  )

}

object S3File {
  def apply(filePath: Parameter[S3Uri]): S3File =
    new S3File(
      id = PipelineObjectId(S3File.getClass),
      filePath = filePath,
      dataFormat = None,
      manifestFilePath = None,
      isCompressed = false,
      isEncrypted = true,
      preconditions = Seq.empty,
      onSuccessAlarms = Seq.empty,
      onFailAlarms = Seq.empty
    )
}

/**
 * Defines data from s3 directory
 */
case class S3Folder private(
  id: PipelineObjectId,
  directoryPath: Parameter[S3Uri],
  dataFormat: Option[DataFormat],
  manifestFilePath: Option[Parameter[S3Uri]],
  isCompressed: Boolean,
  isEncrypted: Boolean,
  preconditions: Seq[Precondition],
  onSuccessAlarms: Seq[SnsAlarm],
  onFailAlarms: Seq[SnsAlarm]
) extends S3DataNode {

  def named(name: String): S3Folder = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String): S3Folder = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withDataFormat(fmt: DataFormat): S3Folder = this.copy(dataFormat = Option(fmt))
  def withManifestFilePath(path: Parameter[S3Uri]): S3Folder = this.copy(manifestFilePath = Option(path))
  def compressed: S3Folder = this.copy(isCompressed = true)
  def unencrypted: S3Folder = this.copy(isEncrypted = false)
  def whenMet(preconditions: Precondition*): S3Folder = this.copy(preconditions = preconditions ++ preconditions)
  def onFail(alarms: SnsAlarm*): S3Folder = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*): S3Folder = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)

  override def toString = directoryPath.toString

  def objects: Iterable[PipelineObject] = dataFormat ++ preconditions ++ onSuccessAlarms ++ onFailAlarms

  lazy val serialize = AdpS3DataNode(
    id = id,
    name = id.toOption,
    directoryPath = Option(directoryPath.toString),
    filePath = None,
    dataFormat = dataFormat.map(_.ref),
    manifestFilePath = manifestFilePath.map(_.toString),
    compression = if (isCompressed) Option("gzip") else None,
    s3EncryptionType = if (isEncrypted) None else Option("NONE"),
    precondition = seqToOption(preconditions)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref)
  )
}

object S3Folder {
  def apply(directoryPath: Parameter[S3Uri]): S3Folder =
    new S3Folder(
      id = PipelineObjectId(S3Folder.getClass),
      directoryPath = directoryPath,
      dataFormat = None,
      manifestFilePath = None,
      isCompressed = false,
      isEncrypted = true,
      preconditions = Seq.empty,
      onSuccessAlarms = Seq.empty,
      onFailAlarms = Seq.empty
    )
}
