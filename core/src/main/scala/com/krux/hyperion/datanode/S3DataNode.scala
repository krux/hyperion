package com.krux.hyperion.datanode

import com.krux.hyperion.aws.AdpS3DataNode
import com.krux.hyperion.common.S3Uri
import com.krux.hyperion.common.PipelineObject
import com.krux.hyperion.dataformat.DataFormat
import com.krux.hyperion.adt.{HS3Uri, HBoolean}
import com.krux.hyperion.common.{ BaseFields, PipelineObjectId }

sealed trait S3DataNode extends Copyable {

  type Self <: S3DataNode

  def s3DataNodeFields: S3DataNodeFields
  def updateS3DataNodeFields(fields: S3DataNodeFields): Self

  def dataFormat = s3DataNodeFields.dataFormat
  def withDataFormat(fmt: DataFormat): Self = updateS3DataNodeFields(
    s3DataNodeFields.copy(dataFormat = Option(fmt))
  )

  def asInput(): String = asInput(1)
  def asInput(n: Integer): String = "${" + s"INPUT${n}_STAGING_DIR}"

  def asOutput(): String = asOutput(1)
  def asOutput(n: Integer): String = "${" + s"OUTPUT${n}_STAGING_DIR}"

  def manifestFilePath = s3DataNodeFields.manifestFilePath
  def withManifestFilePath(path: HS3Uri): Self = updateS3DataNodeFields(
    s3DataNodeFields.copy(manifestFilePath = Option(path))
  )

  def isCompressed = s3DataNodeFields.isCompressed
  def compressed: Self = updateS3DataNodeFields(
    s3DataNodeFields.copy(isCompressed = HBoolean.True)
  )

  def isEncrypted = s3DataNodeFields.isEncrypted
  def unencrypted: Self = updateS3DataNodeFields(
    s3DataNodeFields.copy(isEncrypted = HBoolean.False)
  )

  override def objects: Iterable[PipelineObject] = dataFormat ++ super.objects
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
  baseFields: BaseFields,
  dataNodeFields: DataNodeFields,
  s3DataNodeFields: S3DataNodeFields,
  filePath: HS3Uri
) extends S3DataNode {

  type Self = S3File

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateDataNodeFields(fields: DataNodeFields) = copy(dataNodeFields = fields)
  def updateS3DataNodeFields(fields: S3DataNodeFields) = copy(s3DataNodeFields = fields)

  override def toString = filePath.toString

  lazy val serialize = AdpS3DataNode(
    id = id,
    name = id.toOption,
    directoryPath = None,
    filePath = Option(filePath.serialize),
    dataFormat = dataFormat.map(_.ref),
    manifestFilePath = manifestFilePath.map(_.serialize),
    compression = if (isCompressed) Option("gzip") else None,
    s3EncryptionType = if (isEncrypted) Some("SERVER_SIDE_ENCRYPTION") else Option("NONE"),
    precondition = seqToOption(preconditions)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref)
  )

}

object S3File {
  def apply(filePath: HS3Uri): S3File =
    new S3File(
      baseFields = BaseFields(PipelineObjectId(S3File.getClass)),
      dataNodeFields = DataNodeFields(),
      s3DataNodeFields = S3DataNodeFields(),
      filePath = filePath
    )
}

/**
 * Defines data from s3 directory
 */
case class S3Folder private(
  baseFields: BaseFields,
  dataNodeFields: DataNodeFields,
  s3DataNodeFields: S3DataNodeFields,
  directoryPath: HS3Uri
) extends S3DataNode {

  type Self = S3Folder

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateDataNodeFields(fields: DataNodeFields) = copy(dataNodeFields = fields)
  def updateS3DataNodeFields(fields: S3DataNodeFields) = copy(s3DataNodeFields = fields)

  override def toString = directoryPath.toString

  lazy val serialize = AdpS3DataNode(
    id = id,
    name = id.toOption,
    directoryPath = Option(directoryPath.serialize),
    filePath = None,
    dataFormat = dataFormat.map(_.ref),
    manifestFilePath = manifestFilePath.map(_.serialize),
    compression = if (isCompressed) Option("gzip") else None,
    s3EncryptionType = if (isEncrypted) None else Option("NONE"),
    precondition = seqToOption(preconditions)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref)
  )
}

object S3Folder {

  def apply(directoryPath: HS3Uri): S3Folder =
    new S3Folder(
      baseFields = BaseFields(PipelineObjectId(S3Folder.getClass)),
      dataNodeFields = DataNodeFields(),
      s3DataNodeFields = S3DataNodeFields(),
      directoryPath = directoryPath
    )

}
