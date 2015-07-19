package com.krux.hyperion.precondition

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.AdpS3PrefixNotEmptyPrecondition
import com.krux.hyperion.common.PipelineObjectId

/**
 * A precondition to check that the Amazon S3 objects with the given prefix (represented as a URI) are present.
 *
 * @param s3Prefix  The Amazon S3 prefix to check for existence of objects.
 */
case class S3PrefixNotEmptyPrecondition private (
  id: PipelineObjectId,
  s3Prefix: String,
  preconditionTimeout: Option[String],
  role: String
) extends Precondition {

  lazy val serialize = AdpS3PrefixNotEmptyPrecondition(
    id = id,
    name = id.toOption,
    s3Prefix = s3Prefix,
    preconditionTimeout = preconditionTimeout,
    role = role
  )

}

object S3PrefixNotEmptyPrecondition {
  def apply(s3Prefix: String)(implicit hc: HyperionContext) =
    new S3PrefixNotEmptyPrecondition(
      id = PipelineObjectId("S3PrefixNotEmptyPrecondition"),
      s3Prefix = s3Prefix,
      preconditionTimeout = None,
      role = hc.resourceRole
    )
}
