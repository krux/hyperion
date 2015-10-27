package com.krux.hyperion.precondition

import com.krux.hyperion.adt.{HDuration, HS3Uri, HString}
import com.krux.hyperion.aws.AdpS3PrefixNotEmptyPrecondition
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.HyperionContext

/**
 * A precondition to check that the Amazon S3 objects with the given prefix (represented as a URI) are present.
 *
 * @param s3Prefix  The Amazon S3 prefix to check for existence of objects.
 */
case class S3PrefixNotEmptyPrecondition private (
  id: PipelineObjectId,
  s3Prefix: HS3Uri,
  role: HString,
  preconditionTimeout: Option[HDuration]
) extends Precondition {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withRole(role: HString) = this.copy(role = role)
  def withPreconditionTimeout(timeout: HDuration) = this.copy(preconditionTimeout = Option(timeout))

  lazy val serialize = AdpS3PrefixNotEmptyPrecondition(
    id = id,
    name = id.toOption,
    s3Prefix = s3Prefix.serialize,
    role = role.serialize,
    preconditionTimeout = preconditionTimeout.map(_.serialize)
  )

}

object S3PrefixNotEmptyPrecondition {
  def apply(s3Prefix: HS3Uri)(implicit hc: HyperionContext) =
    new S3PrefixNotEmptyPrecondition(
      id = PipelineObjectId(S3PrefixNotEmptyPrecondition.getClass),
      s3Prefix = s3Prefix,
      role = hc.role,
      preconditionTimeout = None
    )
}
