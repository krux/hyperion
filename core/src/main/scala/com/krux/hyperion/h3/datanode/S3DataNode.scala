package com.krux.hyperion.h3.datanode

import shapeless._

import com.krux.hyperion.aws.AdpS3DataNode
import com.krux.hyperion.common.{ S3Uri, PipelineObjectId }
import com.krux.hyperion.h3.common.PipelineObject
import com.krux.hyperion.h3.dataformat.DataFormat
import com.krux.hyperion.adt.{HS3Uri, HBoolean}
import com.krux.hyperion.h3.common.ObjectFields

sealed trait S3DataNode extends Copyable {

  type Self <: S3DataNode

  def s3DataNodeFieldsLens: Lens[Self, S3DataNodeFields]

  private val dataFormatLens: Lens[Self, Option[DataFormat]] = s3DataNodeFieldsLens >> 'dataFormat
  def dataFormat: Option[DataFormat] = dataFormatLens.get(self)
  def withDataFormat(fmt: DataFormat): Self = dataFormatLens.set(self)(Option(fmt))

  def asInput(): String = asInput(1)
  def asInput(n: Integer): String = "${" + s"INPUT${n}_STAGING_DIR}"

  def asOutput(): String = asOutput(1)
  def asOutput(n: Integer): String = "${" + s"OUTPUT${n}_STAGING_DIR}"

  private val manifestFilePathLens = s3DataNodeFieldsLens >> 'manifestFilePath
  def manifestFilePath = manifestFilePathLens.get(self)
  def withManifestFilePath(path: HS3Uri): Self = manifestFilePathLens.set(self)(Option(path))

  private val isCompressedLens = s3DataNodeFieldsLens >> 'isCompressed
  def isCompressed = isCompressedLens.get(self)
  def compressed: Self = isCompressedLens.set(self)(HBoolean.True)

  private val isEncryptedLens = s3DataNodeFieldsLens >> 'isEncrypted
  def isEncrypted = isEncryptedLens.get(self)
  def unencrypted: Self = isEncryptedLens.set(self)(HBoolean.False)

  def objects: Iterable[PipelineObject] = None // super.objects ++ dataFormat
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
  baseFields: ObjectFields,
  dataNodeFields: DataNodeFields,
  s3DataNodeFields: S3DataNodeFields,
  filePath: HS3Uri
) extends S3DataNode {

  type Self = S3File

  def baseFieldsLens = lens[Self] >> 'baseFields
  def dataNodeFieldsLens = lens[Self] >> 'dataNodeFields
  def s3DataNodeFieldsLens = lens[Self] >> 's3DataNodeFields

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
      baseFields = ObjectFields(PipelineObjectId(S3File.getClass)),
      dataNodeFields = DataNodeFields(),
      s3DataNodeFields = S3DataNodeFields(),
      filePath = filePath
    )
}

/**
 * Defines data from s3 directory
 */
case class S3Folder private(
  baseFields: ObjectFields,
  dataNodeFields: DataNodeFields,
  s3DataNodeFields: S3DataNodeFields,
  directoryPath: HS3Uri
) extends S3DataNode {

  type Self = S3Folder

  def baseFieldsLens = lens[Self] >> 'baseFields
  def dataNodeFieldsLens = lens[Self] >> 'dataNodeFields
  def s3DataNodeFieldsLens = lens[Self] >> 's3DataNodeFields

  override def toString = directoryPath.toString

  // def objects: Iterable[PipelineObject] = dataFormat ++ preconditions ++ onSuccessAlarms ++ onFailAlarms

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
      baseFields = ObjectFields(PipelineObjectId(S3File.getClass)),
      dataNodeFields = DataNodeFields(),
      s3DataNodeFields = S3DataNodeFields(),
      directoryPath = directoryPath
    )

}
