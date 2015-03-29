package com.krux.hyperion.objects

import aws.{AdpS3FileDataNode, AdpS3DirectoryDataNode, AdpJsonSerializer, AdpRef, AdpPrecondition, AdpSnsAlarm}
import com.krux.hyperion.util.PipelineId

trait S3DataNode extends Copyable {

  def asInput(): String = asInput(1)
  def asInput(n: Integer): String = "${" + s"INPUT${n}_STAGING_DIR}"

  def asOutput(): String = asOutput(1)
  def asOutput(n: Integer): String = "${" + s"OUTPUT${n}_STAGING_DIR}"

  def withDataFormat(fmt: DataFormat): S3DataNode
  def forClient(client: String): S3DataNode

}

object S3DataNode {

  def fromPath(s3Path: String): S3DataNode =
    if (s3Path.endsWith("/"))
      S3Folder(PipelineId.generateNewId("S3DataNode"), s3Path, None)
    else
      S3File(PipelineId.generateNewId("S3DataNode"), s3Path, None)

}

/**
 * Defines data from s3
 */
case class S3File(
  id: String,
  filePath: String = "",
  dataFormat: Option[DataFormat] = None,
  preconditions: Seq[Precondition] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onFailAlarms: Seq[SnsAlarm] = Seq()
) extends S3DataNode {

  def forClient(client: String) = this.copy(id = s"${id}_${client}")
  def withDataFormat(fmt: DataFormat) = this.copy(dataFormat = Some(fmt))
  def withFilePath(path: String) = this.copy(filePath = path)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)

  override def objects: Iterable[PipelineObject] = dataFormat

  def serialize = AdpS3FileDataNode(
    id = id,
    name = Some(id),
    compression = None,
    dataFormat = dataFormat.map(f => AdpRef(f.id)),
    filePath = filePath,
    manifestFilePath = None,
    precondition = preconditions match {
      case Seq() => None
      case conditions => Some(conditions.map(precondition => AdpRef[AdpPrecondition](precondition.id)))
    },
    onSuccess = onSuccessAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    },
    onFail = onFailAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    }
  )

}

/**
 * Defines data from s3 directory
 */
case class S3Folder(
  id: String,
  directoryPath: String = "",
  dataFormat: Option[DataFormat] = None,
  preconditions: Seq[Precondition] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onFailAlarms: Seq[SnsAlarm] = Seq()
) extends S3DataNode {

  def forClient(client: String) = this.copy(id = s"${id}_${client}")
  def withDataFormat(fmt: DataFormat) = this.copy(dataFormat = Some(fmt))
  def withDirectoryPath(path: String) = this.copy(directoryPath = path)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)

  override def objects: Iterable[PipelineObject] = dataFormat

  def serialize = AdpS3DirectoryDataNode(
    id = id,
    name = Some(id),
    compression = None,
    dataFormat = dataFormat.map(f => AdpRef(f.id)),
    directoryPath = directoryPath,
    manifestFilePath = None,
    precondition = preconditions match {
      case Seq() => None
      case conditions => Some(conditions.map(precondition => AdpRef[AdpPrecondition](precondition.id)))
    },
    onSuccess = onSuccessAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    },
    onFail = onFailAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    }
  )
}
