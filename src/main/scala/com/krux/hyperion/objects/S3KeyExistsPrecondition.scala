package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpS3KeyExistsPrecondition

/**
 * Checks whether a key exists in an Amazon S3 data node.
 *
 * @param s3Key Amazon S3 key to check for existence.
 */
case class S3KeyExistsPrecondition(
  id: String,
  s3Key: String,
  name: Option[String] = None,
  preconditionTimeout: Option[String] = None,
  role: Option[String] = None
)(
  implicit val hc: HyperionContext
) extends Precondition {

  def serialize = AdpS3KeyExistsPrecondition(
    id = id,
    name = Some(id),
    s3Key = s3Key,
    preconditionTimeout = preconditionTimeout,
    role = role.getOrElse(hc.resourceRole)
  )

}

