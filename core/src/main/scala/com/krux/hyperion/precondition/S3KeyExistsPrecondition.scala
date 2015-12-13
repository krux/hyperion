package com.krux.hyperion.precondition

import com.krux.hyperion.adt.{ HDuration, HS3Uri, HString }
import com.krux.hyperion.aws.AdpS3KeyExistsPrecondition
import com.krux.hyperion.common.{ PipelineObjectId, ObjectFields }
import com.krux.hyperion.HyperionContext

/**
 * Checks whether a key exists in an Amazon S3 data node.
 *
 * @param s3Key Amazon S3 key to check for existence.
 */
case class S3KeyExistsPrecondition private (
  baseFields: ObjectFields,
  preconditionFields: PreconditionFields,
  s3Key: HS3Uri
) extends Precondition {

  type Self = S3KeyExistsPrecondition

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updatePreconditionFields(fields: PreconditionFields) = copy(preconditionFields = fields)

  lazy val serialize = AdpS3KeyExistsPrecondition(
    id = id,
    name = id.toOption,
    s3Key = s3Key.serialize,
    role = role.serialize,
    preconditionTimeout = preconditionTimeout.map(_.serialize)
  )

}

object S3KeyExistsPrecondition {

  def apply(s3Key: HS3Uri)(implicit hc: HyperionContext) = new S3KeyExistsPrecondition(
    baseFields = ObjectFields(PipelineObjectId(S3KeyExistsPrecondition.getClass)),
    preconditionFields = Precondition.defaultPreconditionFields,
    s3Key = s3Key
  )

}
