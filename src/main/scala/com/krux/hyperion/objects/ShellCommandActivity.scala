package com.krux.hyperion.objects

import aws.{AdpJsonSerializer, AdpShellCommandActivity, AdpRef,
  AdpDataNode, AdpActivity, AdpEc2Resource}
import com.krux.hyperion.objects.aws.AdpSnsAlarm

/**
 * Shell command activity
 */
case class ShellCommandActivity(
    id: String,
    runsOn: Ec2Resource,
    command: Option[String] = None,
    scriptUri: Option[String] = None,
    scriptArguments: Seq[String] = Seq(),
    stage: Boolean = true,
    input: Option[S3DataNode] = None,
    output: Option[S3DataNode] = None,
    dependsOn: Seq[PipelineActivity] = Seq(),
    stdout: Option[String] = None,
    stderr: Option[String] = None,
    onFailAlarms: Seq[SnsAlarm] = Seq(),
    onSuccessAlarms: Seq[SnsAlarm] = Seq(),
    onLateActionAlarms: Seq[SnsAlarm] = Seq()
  ) extends PipelineActivity {
  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def withCommand(cmd: String) = this.copy(command = Some(cmd))
  def withScriptUri(uri: String) = this.copy(scriptUri = Some(uri))
  def withArguments(args: String*) = this.copy(scriptArguments = args)

  def staged() = this.copy(stage = true)
  def notStaged() = this.copy(stage = false)

  def withInput(in: S3DataNode) = this.copy(input = Some(in))
  def withOutput(out: S3DataNode) = this.copy(output = Some(out))

  def withStdoutTo(out: String) = this.copy(stdout = Some(out))
  def withStderrTo(err: String) = this.copy(stderr = Some(err))

  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ output ++ dependsOn ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpShellCommandActivity(
      id,
      Some(id),
      command,
      scriptUri,
      scriptArguments match {
        case Seq() => None
        case arguments => Some(arguments)
      },
      input.map(in => AdpRef[AdpDataNode](in.id)),
      output.map(out => AdpRef[AdpDataNode](out.id)),
      stage.toString(),
      stdout,
      stderr,
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
