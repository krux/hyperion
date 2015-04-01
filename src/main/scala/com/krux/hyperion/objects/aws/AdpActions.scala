package com.krux.hyperion.objects.aws

trait AdpAction extends AdpDataPipelineObject

/**
 * An action to trigger the cancellation of a pending or unfinished activity, resource,
 * or data node. AWS Data Pipeline attempts to put the activity, resource, or data node
 * into the CANCELLED state if it does not finish by the lateAfterTimeout value.
 */
class AdpTerminate extends AdpAction {
  val id = "TerminateAction"
  val name = Some("TerminateTasks")
  val `type` = "Terminate"
}

/**
 * Sends an Amazon SNS notification message when an activity fails or finishes successfully.
 *
 * @param message The body text of the Amazon SNS notification. String  Yes
 * @param role  The IAM role to use to create the Amazon SNS alarm. String  Yes
 * @param subject The subject line of the Amazon SNS notification message.  String  Yes
 * @param topicArn  The destination Amazon SNS topic ARN for the message. String  Yes
 */
case class AdpSnsAlarm (
  id: String,
  name: Option[String],
  subject: String,
  message: String,
  topicArn: String,
  role: String
) extends AdpAction {

  val `type` = "SnsAlarm"

}
