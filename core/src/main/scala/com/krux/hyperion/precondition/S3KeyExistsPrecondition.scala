package com.krux.hyperion.precondition

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.AdpS3KeyExistsPrecondition
import com.krux.hyperion.common.{S3Uri, PipelineObjectId}
import com.krux.hyperion.expression.Duration
import com.krux.hyperion.parameter.Parameter

/**
 * Checks whether a key exists in an Amazon S3 data node.
 *
 * @param s3Key Amazon S3 key to check for existence.
 */
case class S3KeyExistsPrecondition private (
  id: PipelineObjectId,
  s3Key: Parameter[S3Uri],
  role: String,
  preconditionTimeout: Option[Parameter[Duration]]
) extends Precondition {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withRole(role: String) = this.copy(role = role)
  def withPreconditionTimeout(timeout: Parameter[Duration]) = this.copy(preconditionTimeout = Option(timeout))

  lazy val serialize = AdpS3KeyExistsPrecondition(
    id = id,
    name = id.toOption,
    s3Key = s3Key.toString,
    role = role,
    preconditionTimeout = preconditionTimeout.map(_.toString)
  )

}

object S3KeyExistsPrecondition {
  def apply(s3Key: Parameter[S3Uri])(implicit hc: HyperionContext) =
    new S3KeyExistsPrecondition(
      id = PipelineObjectId(S3KeyExistsPrecondition.getClass),
      s3Key = s3Key,
      role = hc.role,
      preconditionTimeout = None
    )
}
