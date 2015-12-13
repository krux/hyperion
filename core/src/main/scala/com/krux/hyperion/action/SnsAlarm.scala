package com.krux.hyperion.action

import com.krux.hyperion.common.{ PipelineObjectId, NamedPipelineObject, PipelineObject,
  ObjectFields }
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.{AdpSnsAlarm, AdpRef}

/**
 * Sends an Amazon SNS notification message when an activity fails or finishes successfully.
 */
case class SnsAlarm private (
  baseFields: ObjectFields,
  subject: String,
  message: String,
  topicArn: Option[String],
  role: Option[String]
) extends NamedPipelineObject {

  type Self = SnsAlarm

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)

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
      baseFields = ObjectFields(PipelineObjectId(SnsAlarm.getClass)),
      subject = "",
      message = "",
      topicArn = hc.snsTopic,
      role = hc.snsRole
    )

}
