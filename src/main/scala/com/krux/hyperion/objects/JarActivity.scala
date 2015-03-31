package com.krux.hyperion.objects

import aws.{AdpJsonSerializer, AdpShellCommandActivity, AdpRef,
  AdpDataNode, AdpActivity, AdpEc2Resource, AdpPrecondition}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpSnsAlarm

/**
 * Shell command activity
 */
case class JarActivity(
  id: String,
  runsOn: Ec2Resource,
  jar: Option[String] = None,
  mainClass: Option[String] = None,
  arguments: Seq[String] = Seq(),
  input: Seq[S3DataNode] = Seq(),
  output: Seq[S3DataNode] = Seq(),
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

  def withJar(jar: String) = this.copy(jar = Some(jar))
  def withMainClass(mainClass: String) = this.copy(mainClass = Some(mainClass))
  def withArguments(args: String*) = this.copy(arguments = args)

  def withInput(inputs: S3DataNode*) = this.copy(input = inputs)
  def withOutput(outputs: S3DataNode*) = this.copy(output = outputs)

  def withStdoutTo(out: String) = this.copy(stdout = Some(out))
  def withStderrTo(err: String) = this.copy(stderr = Some(err))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpShellCommandActivity(
    id = id,
    name = Some(id),
    command = None,
    scriptUri = Some(s"${hc.scriptUri}run-jar.sh"),
    scriptArgument = Some(jar.toSeq ++ mainClass.toSeq ++ arguments),
    input = input match {
      case Seq() => None
      case inputs => Some(inputs.map(in => AdpRef[AdpDataNode](in.id)))
    },
    output = output match {
      case Seq() => None
      case outputs => Some(outputs.map(out => AdpRef[AdpDataNode](out.id)))
    },
    stage = "true",
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
