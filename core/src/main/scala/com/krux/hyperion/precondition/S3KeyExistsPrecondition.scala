package com.krux.hyperion.precondition

import com.krux.hyperion.adt.{HDuration, HS3Uri, HString}
import com.krux.hyperion.aws.AdpS3KeyExistsPrecondition
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.HyperionContext

/**
 * Checks whether a key exists in an Amazon S3 data node.
 *
 * @param s3Key Amazon S3 key to check for existence.
 */
case class S3KeyExistsPrecondition private (
  id: PipelineObjectId,
  s3Key: HS3Uri,
  role: HString,
  preconditionTimeout: Option[HDuration]
) extends Precondition {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withRole(role: HString) = this.copy(role = role)
  def withPreconditionTimeout(timeout: HDuration) = this.copy(preconditionTimeout = Option(timeout))

  lazy val serialize = AdpS3KeyExistsPrecondition(
    id = id,
    name = id.toOption,
    s3Key = s3Key.serialize,
    role = role.serialize,
    preconditionTimeout = preconditionTimeout.map(_.serialize)
  )

}

object S3KeyExistsPrecondition {
  def apply(s3Key: HS3Uri)(implicit hc: HyperionContext) =
    new S3KeyExistsPrecondition(
      id = PipelineObjectId(S3KeyExistsPrecondition.getClass),
      s3Key = s3Key,
      role = hc.role,
      preconditionTimeout = None
    )
}
