package com.krux.hyperion.objects

import aws.{AdpJsonSerializer, AdpShellCommandActivity, AdpRef,
  AdpDataNode, AdpActivity, AdpEc2Resource, AdpPrecondition}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.AdpSnsAlarm

/**
 * Shell command activity
 */
case class JarActivity private (
  id: PipelineObjectId,
  runsOn: Ec2Resource,
  jar: Option[String],
  mainClass: Option[String],
  arguments: Seq[String],
  input: Seq[S3DataNode],
  output: Seq[S3DataNode],
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

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = Some(id),
    command = None,
    scriptUri = Some(s"${hc.scriptUri}run-jar.sh"),
    scriptArgument = Some(jar.toSeq ++ mainClass.toSeq ++ arguments),
    input = seqToOption(input)(in => AdpRef(in.serialize)),
    output = seqToOption(output)(out => AdpRef(out.serialize)),
    stage = "true",
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

object JarActivity {
  def apply(runsOn: Ec2Resource)(implicit hc: HyperionContext) =
    new JarActivity(
      id = PipelineObjectId("JarActivity"),
      runsOn = runsOn,
      jar = None,
      mainClass = None,
      arguments = Seq(),
      input = Seq(),
      output = Seq(),
      stdout = None,
      stderr = None,
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )
}
