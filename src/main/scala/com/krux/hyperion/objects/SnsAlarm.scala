package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpSnsAlarm

case class SnsAlarm(
  id: String,
  subject: String = "",
  message: String = "",
  topicArn: Option[String] = None,
  role: Option[String] = None
)(
  implicit val hc: HyperionContext
) extends PipelineObject {

  def withSubject(subject: String) = this.copy(subject = subject)
  def withMessage(message: String) = this.copy(message = message)
  def withTopicArn(topicArn: String) = this.copy(topicArn = Some(topicArn))
  def withRole(role: String) = this.copy(role = Some(role))

  def serialize = new AdpSnsAlarm(
    id,
    Some(id),
    subject,
    message,
    topicArn.getOrElse(hc.snsTopic),
    role.getOrElse(hc.snsRole)
  )

}
