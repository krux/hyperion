package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpS3KeyExistsPrecondition

/**
 * Checks whether a key exists in an Amazon S3 data node.
 *
 * @param s3Key Amazon S3 key to check for existence.
 */
case class S3KeyExistsPrecondition private (
  id: PipelineObjectId,
  s3Key: String,
  preconditionTimeout: Option[String],
  role: Option[String]
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

object S3KeyExistsPrecondition {
  def apply(s3Key: String)(implicit hc: HyperionContext) =
    new S3KeyExistsPrecondition(
      id = PipelineObjectId("S3KeyExistsPrecondition"),
      s3Key = s3Key,
      preconditionTimeout = None,
      role = None
    )
}
