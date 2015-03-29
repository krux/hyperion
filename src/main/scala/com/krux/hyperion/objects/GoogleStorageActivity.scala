package com.krux.hyperion.objects

import aws.{AdpJsonSerializer, AdpShellCommandActivity, AdpRef,
  AdpDataNode, AdpActivity, AdpEc2Resource, AdpPrecondition}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpSnsAlarm

trait GoogleStorageActivity extends PipelineActivity

/**
 * Google Storage Download activity
 */
case class GoogleStorageDownloadActivity(
  id: String,
  runsOn: Ec2Resource,
  input: String = "",
  output: Option[S3DataNode] = None,
  botoConfigUrl: String = "",
  dependsOn: Seq[PipelineActivity] = Seq(),
  preconditions: Seq[Precondition] = Seq(),
  onFailAlarms: Seq[SnsAlarm] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onLateActionAlarms: Seq[SnsAlarm] = Seq()
)(
  implicit val hc: HyperionContext
) extends GoogleStorageActivity {

  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def withBotoConfigUrl(url: String) = this.copy(botoConfigUrl = url)
  def withInput(path: String) = this.copy(input = path)
  def withOutput(out: S3DataNode) = this.copy(output = Some(out))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpShellCommandActivity(
    id = id,
    name = Some(id),
    command = None,
    scriptUri = Some(s"${hc.scriptUri}gsutil/gsutil_download.sh"),
    scriptArgument = Some(Seq(botoConfigUrl, input)),
    input = None,
    output = output.map(out => AdpRef[AdpDataNode](out.id)),
    stage = "true",
    stdout = None,
    stderr = None,
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

/**
 * Google Storage Upload activity
 */
case class GoogleStorageUploadActivity(
  id: String,
  runsOn: Ec2Resource,
  input: Option[S3DataNode] = None,
  output: String = "",
  botoConfigUrl: String = "",
  dependsOn: Seq[PipelineActivity] = Seq(),
  preconditions: Seq[Precondition] = Seq(),
  onFailAlarms: Seq[SnsAlarm] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onLateActionAlarms: Seq[SnsAlarm] = Seq()
)(
  implicit val hc: HyperionContext
) extends GoogleStorageActivity {

  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def withBotoConfigUrl(url: String) = this.copy(botoConfigUrl = url)
  def withInput(in: S3DataNode) = this.copy(input = Some(in))
  def withOutput(path: String) = this.copy(output = path)

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ dependsOn

  def serialize = AdpShellCommandActivity(
    id = id,
    name = Some(id),
    command = None,
    scriptUri = Some(s"${hc.scriptUri}gsutil/gsutil_upload.sh"),
    scriptArgument = Some(Seq(botoConfigUrl, output)),
    input = input.map(in => AdpRef[AdpDataNode](in.id)),
    output = None,
    stage = "true",
    stdout = None,
    stderr = None,
    dependsOn = dependsOn match {
      case Seq() => None
      case deps => Some(deps.map(act => AdpRef[AdpActivity](act.id)))
    },
    runsOn = AdpRef[AdpEc2Resource](runsOn.id),
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
