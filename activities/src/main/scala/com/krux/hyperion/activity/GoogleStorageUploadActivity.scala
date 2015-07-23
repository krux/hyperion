package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.Ec2Resource

/**
 * Google Storage Upload activity
 */
case class GoogleStorageUploadActivity private (
  id: PipelineObjectId,
  runsOn: Ec2Resource,
  scriptUri: String,
  input: Option[S3DataNode],
  output: String,
  botoConfigUrl: String,
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm]
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
    scriptUri = Option(scriptUri),
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
      scriptUri = s"${hc.scriptUri}activities/gsutil-upload.sh",
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
