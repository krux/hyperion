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
  preconditionTimeout: Option[String],
  role: String
) extends Precondition {

  def withPreconditionTimeOut(timeout: String) = this.copy(preconditionTimeout = Option(timeout))
  def withRole(role: String) = this.copy(role = role)

  lazy val serialize = AdpS3KeyExistsPrecondition(
    id = id,
    name = id.toOption,
    s3Key = s3Key,
    preconditionTimeout = preconditionTimeout,
    role = role
  )

}

object S3KeyExistsPrecondition {
  def apply(s3Key: String)(implicit hc: HyperionContext) =
    new S3KeyExistsPrecondition(
      id = PipelineObjectId("S3KeyExistsPrecondition"),
      s3Key = s3Key,
      preconditionTimeout = None,
      role = hc.role
    )
}
