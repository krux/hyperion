package com.krux.hyperion.objects

import aws.{AdpS3FileDataNode, AdpS3DirectoryDataNode, AdpJsonSerializer, AdpRef}
import com.krux.hyperion.util.PipelineId

trait S3DataNode extends DataNode {

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
    dataFormat: Option[DataFormat] = None
  ) extends S3DataNode {

  def forClient(client: String) = this.copy(id = s"${id}_${client}")
  def withDataFormat(fmt: DataFormat) = this.copy(dataFormat = Some(fmt))
  def withFilePath(path: String) = this.copy(filePath = path)

  override def objects: Iterable[PipelineObject] = dataFormat

  def serialize = AdpS3FileDataNode(
      id,
      Some(id),
      None,
      dataFormat.map(f => AdpRef(f.id)),
      filePath,
      None
    )

}

/**
 * Defines data from s3 directory
 */
case class S3Folder(
    id: String,
    directoryPath: String = "",
    dataFormat: Option[DataFormat] = None
  ) extends S3DataNode {

  def forClient(client: String) = this.copy(id = s"${id}_${client}")
  def withDataFormat(fmt: DataFormat) = this.copy(dataFormat = Some(fmt))
  def withDirectoryPath(path: String) = this.copy(directoryPath = path)

  override def objects: Iterable[PipelineObject] = dataFormat

  def serialize = AdpS3DirectoryDataNode(
      id,
      Some(id),
      None,
      dataFormat.map(f => AdpRef(f.id)),
      directoryPath,
      None
    )
}
