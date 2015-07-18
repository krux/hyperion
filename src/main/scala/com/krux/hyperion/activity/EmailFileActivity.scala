package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.Ec2Resource

case class EmailFileActivity private (
  id: PipelineObjectId,
  runsOn: Ec2Resource,
  scriptUri: Option[String],
  filename: Option[String],
  from: Option[String],
  to: Option[String],
  cc: Option[String],
  subject: Option[String],
  body: Option[String],
  input: Seq[S3DataNode],
  stdout: Option[String],
  stderr: Option[String],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm]
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withFilename(filename: String) = this.copy(filename = Option(filename))
  def withFrom(from: String) = this.copy(from = Option(from))
  def withTo(to: String) = this.copy(to = Option(to))
  def withCc(cc: String) = this.copy(cc = Option(cc))
  def withSubject(subject: String) = this.copy(subject = Option(subject))
  def withBody(body: String) = this.copy(body = Option(body))

  def withInput(inputs: S3DataNode*) = this.copy(input = input ++ inputs)

  def withStdoutTo(out: String) = this.copy(stdout = Option(out))
  def withStderrTo(err: String) = this.copy(stderr = Option(err))

  private[hyperion] def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def arguments: Seq[String] = Seq(filename, from, subject, body, to).map(_.get) ++ cc.toSeq

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = scriptUri,
    scriptArgument = Option(arguments),
    input = seqToOption(input)(_.ref),
    output = None,
    stage = "true",
    stdout = stdout,
    stderr = stderr,
    runsOn = runsOn.ref,
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref)
  )

}

object EmailFileActivity {

  def apply(runsOn: Ec2Resource)(implicit hc: HyperionContext) =
    new EmailFileActivity(
      id = PipelineObjectId("EmailFileActivity"),
      runsOn = runsOn,
      scriptUri = Option(s"${hc.scriptUri}email-file.sh"),
      filename = None,
      from = None,
      to = None,
      cc = None,
      subject = None,
      body = None,
      input = Seq(),
      stdout = None,
      stderr = None,
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )

}
