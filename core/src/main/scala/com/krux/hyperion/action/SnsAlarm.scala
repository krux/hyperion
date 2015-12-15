package com.krux.hyperion.action

import com.krux.hyperion.adt.HString
import com.krux.hyperion.common.{ PipelineObjectId, NamedPipelineObject, PipelineObject,
  BaseFields }
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.{AdpSnsAlarm, AdpRef}

/**
 * Sends an Amazon SNS notification message when an activity fails or finishes successfully.
 */
case class SnsAlarm private (
  baseFields: BaseFields,
  subject: HString,
  message: HString,
  role: HString,
  topicArn: HString
) extends NamedPipelineObject {

  type Self = SnsAlarm

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)

  def objects: Iterable[PipelineObject] = None

  def withSubject(subject: HString) = copy(subject = subject)
  def withMessage(message: HString) = copy(message = message)
  def withRole(role: HString) = copy(role = role)
  def withTopicArn(arn: HString) = copy(topicArn = arn)

  lazy val serialize = new AdpSnsAlarm(
    id = id,
    name = id.toOption,
    subject = subject.serialize,
    message = message.serialize,
    topicArn = topicArn.serialize,
    role = role.serialize
  )

  def ref: AdpRef[AdpSnsAlarm] = AdpRef(serialize)

}

object SnsAlarm {

  def apply()(implicit hc: HyperionContext) = new SnsAlarm(
    baseFields = BaseFields(PipelineObjectId(SnsAlarm.getClass)),
    subject = "",
    message = "",
    topicArn = hc.snsTopic.getOrElse("").toString,
    role = hc.snsRole.get
  )

}
