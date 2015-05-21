package com.krux.hyperion.activity

import com.krux.hyperion.common.{PipelineObjectId, PipelineObject}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.Ec2Resource

trait GoogleStorageActivity extends PipelineActivity

/**
 * Google Storage Download activity
 */
case class GoogleStorageDownloadActivity private (
  id: PipelineObjectId,
  runsOn: Ec2Resource,
  input: String,
  output: Option[S3DataNode],
  botoConfigUrl: String,
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm]
)(
  implicit val hc: HyperionContext
) extends GoogleStorageActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withBotoConfigUrl(url: String) = this.copy(botoConfigUrl = url)
  def withInput(path: String) = this.copy(input = path)
  def withOutput(out: S3DataNode) = this.copy(output = Option(out))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = Option(s"${hc.scriptUri}gsutil/gsutil_download.sh"),
    scriptArgument = Option(Seq(botoConfigUrl, input)),
    input = None,
    output = output.map(out => Seq(out.ref)),
    stage = "true",
    stdout = None,
    stderr = None,
    runsOn = runsOn.ref,
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref)
  )

}

object GoogleStorageDownloadActivity {
  def apply(runsOn: Ec2Resource)(implicit hc: HyperionContext) =
    new GoogleStorageDownloadActivity(
      id = PipelineObjectId("GoogleStorageDownloadActivity"),
      runsOn = runsOn,
      input = "",
      output = None,
      botoConfigUrl = "",
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )
}

/**
 * Google Storage Upload activity
 */
case class GoogleStorageUploadActivity private (
  id: PipelineObjectId,
  runsOn: Ec2Resource,
  input: Option[S3DataNode],
  output: String,
  botoConfigUrl: String,
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm]
)(
  implicit val hc: HyperionContext
) extends GoogleStorageActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withBotoConfigUrl(url: String) = this.copy(botoConfigUrl = url)
  def withInput(in: S3DataNode) = this.copy(input = Option(in))
  def withOutput(path: String) = this.copy(output = path)

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions ++ preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ dependsOn

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = Option(s"${hc.scriptUri}gsutil/gsutil_upload.sh"),
    scriptArgument = Option(Seq(botoConfigUrl, output)),
    input = input.map(in => Seq(in.ref)),
    output = None,
    stage = "true",
    stdout = None,
    stderr = None,
    dependsOn = seqToOption(dependsOn)(_.ref),
    runsOn = runsOn.ref,
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref)
  )

}

object GoogleStorageUploadActivity extends RunnableObject {
  def apply(runsOn: Ec2Resource)(implicit hc: HyperionContext) =
    new GoogleStorageUploadActivity(
      id = PipelineObjectId("GoogleStorageUploadActivity"),
      runsOn = runsOn,
      input = None,
      output = "",
      botoConfigUrl = "",
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )
}
