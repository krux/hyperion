package com.krux.hyperion.objects

import aws.{AdpJsonSerializer, AdpShellCommandActivity, AdpRef,
  AdpDataNode, AdpActivity, AdpEc2Resource}
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
  onFailAlarms: Seq[SnsAlarm] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onLateActionAlarms: Seq[SnsAlarm] = Seq()
)(
  implicit val hc: HyperionContext
) extends GoogleStorageActivity {

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def withBotoConfigUrl(url: String) = this.copy(botoConfigUrl = url)
  def withInput(path: String) = this.copy(input = path)
  def withOutput(out: S3DataNode) = this.copy(output = Some(out))

  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ output ++ dependsOn ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpShellCommandActivity(
      id,
      Some(id),
      None,
      Some(s"${hc.scriptUri}gsutil/gsutil_download.sh"),
      Some(Seq(botoConfigUrl, input)),
      None,
      output.map(out => AdpRef[AdpDataNode](out.id)),
      "true",
      None,
      None,
      dependsOn match {
        case Seq() => None
        case deps => Some(deps.map(act => AdpRef[AdpActivity](act.id)))
      },
      AdpRef[AdpEc2Resource](runsOn.id),
      onFailAlarms match {
        case Seq() => None
        case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
      },
      onSuccessAlarms match {
        case Seq() => None
        case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
      },
      onLateActionAlarms match {
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
    onFailAlarms: Seq[SnsAlarm] = Seq(),
    onSuccessAlarms: Seq[SnsAlarm] = Seq(),
    onLateActionAlarms: Seq[SnsAlarm] = Seq()
  )(
    implicit val hc: HyperionContext
  ) extends GoogleStorageActivity {
  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def withBotoConfigUrl(url: String) = this.copy(botoConfigUrl = url)
  def withInput(in: S3DataNode) = this.copy(input = Some(in))
  def withOutput(path: String) = this.copy(output = path)

  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ dependsOn

  def serialize = AdpShellCommandActivity(
      id,
      Some(id),
      None,
      Some(s"${hc.scriptUri}gsutil/gsutil_upload.sh"),
      Some(Seq(botoConfigUrl, output)),
      input.map(in => AdpRef[AdpDataNode](in.id)),
      None,
      "true",
      None,
      None,
      dependsOn match {
        case Seq() => None
        case deps => Some(deps.map(act => AdpRef[AdpActivity](act.id)))
      },
      AdpRef[AdpEc2Resource](runsOn.id),
      onFailAlarms match {
        case Seq() => None
        case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
      },
      onSuccessAlarms match {
        case Seq() => None
        case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
      },
      onLateActionAlarms match {
        case Seq() => None
        case alarms => Some(alarms.map(alarm => AdpRef[AdpSnsAlarm](alarm.id)))
      }
    )
}
