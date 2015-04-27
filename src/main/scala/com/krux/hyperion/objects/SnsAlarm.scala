package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.{AdpSnsAlarm, AdpRef}

case class SnsAlarm private (
  id: PipelineObjectId,
  subject: String,
  message: String,
  topicArn: String,
  role: String
)(
  implicit val hc: HyperionContext
) extends PipelineObject {

  def withSubject(subject: String) = this.copy(subject = subject)
  def withMessage(message: String) = this.copy(message = message)
  def withTopicArn(topicArn: String) = this.copy(topicArn = topicArn)
  def withRole(role: String) = this.copy(role = role)

  lazy val serialize = new AdpSnsAlarm(
    id = id,
    name = id.toOption,
    subject = subject,
    message = message,
    topicArn = topicArn,
    role = role
  )

  def ref: AdpRef[AdpSnsAlarm] = AdpRef(serialize)

}

object SnsAlarm {
  def apply()(implicit hc: HyperionContext) =
    new SnsAlarm(
      id = PipelineObjectId("SnsAlarm"),
      subject = "",
      message = "",
      topicArn = hc.snsTopic,
      role = hc.snsRole
    )
}
