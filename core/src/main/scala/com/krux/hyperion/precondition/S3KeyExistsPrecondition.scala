package com.krux.hyperion.precondition

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.AdpS3KeyExistsPrecondition
import com.krux.hyperion.common.PipelineObjectId

/**
 * Checks whether a key exists in an Amazon S3 data node.
 *
 * @param s3Key Amazon S3 key to check for existence.
 */
case class S3KeyExistsPrecondition private (
  id: PipelineObjectId,
  s3Key: String,
  role: String,
  preconditionTimeout: Option[String]
) extends Precondition {

  def withPreconditionTimeout(timeout: String) = this.copy(preconditionTimeout = Option(timeout))
  def withRole(role: String) = this.copy(role = role)

  lazy val serialize = AdpS3KeyExistsPrecondition(
    id = id,
    name = id.toOption,
    s3Key = s3Key,
    role = role,
    preconditionTimeout = preconditionTimeout
  )

}

object S3KeyExistsPrecondition {
  def apply(s3Key: String)(implicit hc: HyperionContext) =
    new S3KeyExistsPrecondition(
      id = PipelineObjectId(S3KeyExistsPrecondition.getClass),
      s3Key = s3Key,
      role = hc.role,
      preconditionTimeout = None
    )
}
