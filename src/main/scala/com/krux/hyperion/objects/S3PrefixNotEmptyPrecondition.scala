package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpS3PrefixNotEmptyPrecondition

/**
 * A precondition to check that the Amazon S3 objects with the given prefix (represented as a URI) are present.
 *
 * @param s3Prefix  The Amazon S3 prefix to check for existence of objects.
 */
case class S3PrefixNotEmptyPrecondition private (
  id: PipelineObjectId,
  s3Prefix: String,
  preconditionTimeout: Option[String],
  role: Option[String]
)(
  implicit val hc: HyperionContext
) extends Precondition {

  def serialize = AdpS3PrefixNotEmptyPrecondition(
    id = id,
    name = Some(id),
    s3Prefix = s3Prefix,
    preconditionTimeout = preconditionTimeout,
    role = role.getOrElse(hc.resourceRole)
  )

}

object S3PrefixNotEmptyPrecondition {
  def apply(s3Prefix: String)(implicit hc: HyperionContext) =
    new S3PrefixNotEmptyPrecondition(
      id = PipelineObjectId("S3PrefixNotEmptyPrecondition"),
      s3Prefix = s3Prefix,
      preconditionTimeout = None,
      role = None
    )
}
