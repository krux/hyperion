package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpShellCommandActivity
import com.krux.hyperion.objects.aws.AdpRef
import com.krux.hyperion.objects.aws.AdpEc2Resource
import com.krux.hyperion.objects.aws.AdpActivity
import com.krux.hyperion.objects.aws.AdpPrecondition
import com.krux.hyperion.objects.aws.AdpSnsAlarm

/**
 * Activity to recursively delete files in an S3 path.
 */
case class DeleteS3PathActivity(
  id: String,
  s3Path: String,
  runsOn: Ec2Resource,
  stdout: Option[String] = None,
  stderr: Option[String] = None,
  dependsOn: Seq[PipelineActivity] = Seq(),
  preconditions: Seq[Precondition] = Seq(),
  onFailAlarms: Seq[SnsAlarm] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onLateActionAlarms: Seq[SnsAlarm] = Seq()
)(
  implicit val hc: HyperionContext
) extends PipelineActivity {

  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def withStdoutTo(out: String) = this.copy(stdout = Some(out))
  def withStderrTo(err: String) = this.copy(stderr = Some(err))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpShellCommandActivity(
    id = id,
    name = Some(id),
    command = Some(s"aws s3 rm --recursive $s3Path"),
    scriptUri = None,
    scriptArgument = None,
    input = None,
    output = None,
    stage = "false",
    stdout = stdout,
    stderr = stderr,
    runsOn = AdpRef[AdpEc2Resource](runsOn.id),
    dependsOn = dependsOn match {
      case Seq() => None
      case deps => Some(deps.map(act => AdpRef[AdpActivity](act.id)))
    },
    precondition = preconditions match {
      case Seq() => None
      case preconditions => Some(preconditions.map(precondition => AdpRef[AdpPrecondition](precondition.id)))
    },
    onFail = onFailAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    },
    onSuccess = onSuccessAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    },
    onLateAction = onLateActionAlarms match {
      case Seq() => None
      case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
    }
  )

}
