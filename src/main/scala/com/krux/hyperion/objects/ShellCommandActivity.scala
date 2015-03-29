package com.krux.hyperion.objects

import aws.{AdpJsonSerializer, AdpShellCommandActivity, AdpRef,
  AdpDataNode, AdpActivity, AdpEc2Resource, AdpPrecondition}
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
  stdout: Option[String] = None,
  stderr: Option[String] = None,
  dependsOn: Seq[PipelineActivity] = Seq(),
  preconditions: Seq[Precondition] = Seq(),
  onFailAlarms: Seq[SnsAlarm] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onLateActionAlarms: Seq[SnsAlarm] = Seq()
) extends PipelineActivity {

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

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ preconditions ++ input ++ output ++ dependsOn ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpShellCommandActivity(
    id = id,
    name = Some(id),
    command = command,
    scriptUri = scriptUri,
    scriptArgument = scriptArguments match {
      case Seq() => None
      case arguments => Some(arguments)
    },
    input = input.map(in => AdpRef[AdpDataNode](in.id)),
    output = output.map(out => AdpRef[AdpDataNode](out.id)),
    stage = stage.toString(),
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
