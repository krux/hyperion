package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpExistsPrecondition

/**
 * Checks whether a data node object exists.
 */
case class ExistsPrecondition private (
  id: PipelineObjectId,
  preconditionTimeout: Option[String],
  role: Option[String]
)(
  implicit val hc: HyperionContext
) extends Precondition {

  def withPreconditionTimeOut(timeout: String) = this.copy(preconditionTimeout = Option(timeout))
  def withRole(role: String) = this.copy(role = Option(role))

  lazy val serialize = AdpExistsPrecondition(
    id = id,
    name = id.toOption,
    preconditionTimeout = preconditionTimeout,
    role = role.getOrElse(hc.resourceRole)
  )

}

object ExistsPrecondition {
  def apply()(implicit hc: HyperionContext) =
    new ExistsPrecondition(
      id = PipelineObjectId("ExistsPrecondition"),
      preconditionTimeout = None,
      role = None
    )
}
