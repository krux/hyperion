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
case class DeleteS3PathActivity private (
  id: PipelineObjectId,
  s3Path: String,
  runsOn: Ec2Resource,
  stdout: Option[String],
  stderr: Option[String],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm]
)(
  implicit val hc: HyperionContext
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))

  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withStdoutTo(out: String) = this.copy(stdout = Some(out))
  def withStderrTo(err: String) = this.copy(stderr = Some(err))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpShellCommandActivity(
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
    runsOn = AdpRef(runsOn.serialize),
    dependsOn = seqToOption(dependsOn)(act => AdpRef(act.serialize)),
    precondition = seqToOption(preconditions)(precondition => AdpRef(precondition.serialize)),
    onFail = seqToOption(onFailAlarms)(alarm => AdpRef(alarm.serialize)),
    onSuccess = seqToOption(onSuccessAlarms)(alarm => AdpRef(alarm.serialize)),
    onLateAction = seqToOption(onLateActionAlarms)(alarm => AdpRef(alarm.serialize))
  )

}

object DeleteS3PathActivity {
  def apply(s3Path: String, runsOn: Ec2Resource)(implicit hc: HyperionContext) =
    new DeleteS3PathActivity(
      id = PipelineObjectId("DeleteS3PathActivity"),
      s3Path = s3Path,
      runsOn = runsOn,
      stdout = None,
      stderr = None,
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )
}
