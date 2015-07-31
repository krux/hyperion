package com.krux.hyperion.datanode

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.{AdpS3FileDataNode, AdpS3DirectoryDataNode}
import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.dataformat.DataFormat
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.WorkerGroup

trait S3DataNode extends Copyable {

  def asInput(): String = asInput(1)
  def asInput(n: Integer): String = "${" + s"INPUT${n}_STAGING_DIR}"

  def asOutput(): String = asOutput(1)
  def asOutput(n: Integer): String = "${" + s"OUTPUT${n}_STAGING_DIR}"

  def withDataFormat(fmt: DataFormat): S3DataNode
  def groupedBy(client: String): S3DataNode

}

object S3DataNode {

  def fromPath(s3Path: String): S3DataNode =
    if (s3Path.endsWith("/")) S3Folder(s3Path)
    else S3File(s3Path)

}

/**
 * Defines data from s3
 */
case class S3File(
  id: PipelineObjectId,
  filePath: String,
  dataFormat: Option[DataFormat],
  workerGroup: Option[WorkerGroup],
  preconditions: Seq[Precondition],
  onSuccessAlarms: Seq[SnsAlarm],
  onFailAlarms: Seq[SnsAlarm]
) extends S3DataNode {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withDataFormat(fmt: DataFormat) = this.copy(dataFormat = Option(fmt))
  def withFilePath(path: String) = this.copy(filePath = path)

  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)

  override def toString: String = filePath

  override def objects: Iterable[PipelineObject] = dataFormat

  lazy val serialize = AdpS3FileDataNode(
    id = id,
    name = id.toOption,
    compression = None,
    dataFormat = dataFormat.map(_.ref),
    filePath = filePath,
    manifestFilePath = None,
    workerGroup = workerGroup.map(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref)
  )

}

object S3File {
  def apply(filePath: String): S3File = apply(filePath, None)

  def apply(filePath: String, workerGroup: WorkerGroup): S3File = apply(filePath, Option(workerGroup))

  def apply(filePath: String, runsOn: Option[WorkerGroup]): S3File =
    new S3File(
      id = PipelineObjectId(S3File.getClass),
      filePath = filePath,
      dataFormat = None,
      workerGroup = runsOn,
      preconditions = Seq(),
      onSuccessAlarms = Seq(),
      onFailAlarms = Seq()
    )
}

/**
 * Defines data from s3 directory
 */
case class S3Folder(
  id: PipelineObjectId,
  directoryPath: String = "",
  dataFormat: Option[DataFormat] = None,
  workerGroup: Option[WorkerGroup],
  preconditions: Seq[Precondition] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onFailAlarms: Seq[SnsAlarm] = Seq()
) extends S3DataNode {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withDataFormat(fmt: DataFormat) = this.copy(dataFormat = Option(fmt))
  def withDirectoryPath(path: String) = this.copy(directoryPath = path)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions ++ preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)

  override def toString: String = directoryPath

  override def objects: Iterable[PipelineObject] = dataFormat

  lazy val serialize = AdpS3DirectoryDataNode(
    id = id,
    name = id.toOption,
    compression = None,
    dataFormat = dataFormat.map(_.ref),
    directoryPath = directoryPath,
    manifestFilePath = None,
    workerGroup = workerGroup.map(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref)
  )
}

object S3Folder {
  def apply(directoryPath: String): S3Folder = apply(directoryPath, None)

  def apply(directoryPath: String, workerGroup: WorkerGroup): S3Folder = apply(directoryPath, Option(workerGroup))

  def apply(directoryPath: String, runsOn: Option[WorkerGroup]): S3Folder =
    new S3Folder(
      id = PipelineObjectId(S3Folder.getClass),
      directoryPath = directoryPath,
      dataFormat = None,
      workerGroup = runsOn,
      preconditions = Seq(),
      onSuccessAlarms = Seq(),
      onFailAlarms = Seq()
    )
}