package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpS3PrefixNotEmptyPrecondition

/**
 * A precondition to check that the Amazon S3 objects with the given prefix (represented as a URI) are present.
 *
 * @param s3Prefix  The Amazon S3 prefix to check for existence of objects.
 */
case class S3PrefixNotEmptyPrecondition(
  id: String,
  s3Prefix: String,
  preconditionTimeout: Option[String] = None,
  role: Option[String] = None
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
