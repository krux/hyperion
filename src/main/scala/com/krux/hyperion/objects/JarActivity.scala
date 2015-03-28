package com.krux.hyperion.objects

import aws.{AdpJsonSerializer, AdpShellCommandActivity, AdpRef,
  AdpDataNode, AdpActivity, AdpEc2Resource}
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
    dependsOn: Seq[PipelineActivity] = Seq(),
    input: Option[S3DataNode] = None,
    output: Option[S3DataNode] = None,
    stdout: Option[String] = None,
    stderr: Option[String] = None,
    onFailAlarms: Seq[SnsAlarm] = Seq(),
    onSuccessAlarms: Seq[SnsAlarm] = Seq(),
    onLateActionAlarms: Seq[SnsAlarm] = Seq()
  )(
    implicit val hc: HyperionContext
  ) extends PipelineActivity {
  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def withJar(jar: String) = this.copy(jar = Some(jar))
  def withMainClass(mainClass: String) = this.copy(mainClass = Some(mainClass))
  def withArguments(args: String*) = this.copy(arguments = args)

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
      None,
      Some(s"${hc.scriptUri}run-jar.sh"),
      Some(jar.toSeq ++ mainClass.toSeq ++ arguments),
      input.map(in => AdpRef[AdpDataNode](in.id)),
      output.map(out => AdpRef[AdpDataNode](out.id)),
      "true",
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
