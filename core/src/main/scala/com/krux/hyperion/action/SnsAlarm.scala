package com.krux.hyperion.action

import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.{AdpSnsAlarm, AdpRef}

/**
 * Sends an Amazon SNS notification message when an activity fails or finishes successfully.
 */
case class SnsAlarm private (
  id: PipelineObjectId,
  subject: String,
  message: String,
  topicArn: Option[String],
  role: Option[String]
) extends PipelineObject {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withSubject(subject: String) = this.copy(subject = subject)
  def withMessage(message: String) = this.copy(message = message)
  def withTopicArn(topicArn: String) = this.copy(topicArn = Some(topicArn))
  def withRole(role: String) = this.copy(role = Some(role))

  def objects: Iterable[PipelineObject] = None

  lazy val serialize = new AdpSnsAlarm(
    id = id,
    name = id.toOption,
    subject = subject,
    message = message,
    topicArn = topicArn.get,
    role = role.get
  )

  def ref: AdpRef[AdpSnsAlarm] = AdpRef(serialize)

}

object SnsAlarm {
  def apply()(implicit hc: HyperionContext) =
    new SnsAlarm(
      id = PipelineObjectId(SnsAlarm.getClass),
      subject = "",
      message = "",
      topicArn = hc.snsTopic,
      role = hc.snsRole
    )
}
