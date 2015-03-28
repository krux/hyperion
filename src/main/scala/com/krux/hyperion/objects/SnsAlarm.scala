package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpSnsAlarm

case class SnsAlarm(
  id: String,
  subject: String = "",
  message: String = "",
  topicArn: String = "",
  role: String = ""
)(
  implicit val hc: HyperionContext
) extends PipelineObject {

  def withSubject(subject: String) = this.copy(subject = subject)
  def withMessage(message: String) = this.copy(message = message)
  def withTopicArn(topicArn: String) = this.copy(topicArn = topicArn)
  def withRole(role: String) = this.copy(role = role)

  def serialize = new AdpSnsAlarm(
    id,
    Some(id),
    subject,
    message,
    topicArn,
    role
  )

}