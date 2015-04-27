package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpShellCommandActivity
import com.krux.hyperion.HyperionContext

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

  def withJar(jar: String) = this.copy(jar = Option(jar))
  def withMainClass(mainClass: String) = this.copy(mainClass = Option(mainClass))
  def withArguments(args: String*) = this.copy(arguments = args)

  def withInput(inputs: S3DataNode*) = this.copy(input = inputs)
  def withOutput(outputs: S3DataNode*) = this.copy(output = outputs)

  def withStdoutTo(out: String) = this.copy(stdout = Option(out))
  def withStderrTo(err: String) = this.copy(stderr = Option(err))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = Option(s"${hc.scriptUri}run-jar.sh"),
    scriptArgument = Option(jar.toSeq ++ mainClass.toSeq ++ arguments),
    input = seqToOption(input)(_.ref),
    output = seqToOption(output)(_.ref),
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
