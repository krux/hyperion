package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.Ec2Resource

/**
 * Shell command activity that runs a given python script
 */
case class PythonActivity private (
  id: PipelineObjectId,
  runsOn: Ec2Resource,
  scriptUri: Option[String],
  pythonScriptUri: Option[String],
  pythonScript: Option[String],
  pythonModule: Option[String],
  pythonRequirements: Option[String],
  pipIndexUrl: Option[String],
  pipExtraIndexUrls: Seq[String],
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
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withScriptUri(pythonScriptUri: String) = this.copy(pythonScriptUri = Option(pythonScriptUri))
  def withScript(pythonScript: String) = this.copy(pythonScript = Option(pythonScript))
  def withModule(pythonModule: String) = this.copy(pythonModule = Option(pythonModule))
  def withRequirements(pythonRequirements: String) = this.copy(pythonRequirements = Option(pythonRequirements))
  def withIndexUrl(indexUrl: String) = this.copy(pipIndexUrl = Option(indexUrl))
  def withExtraIndexUrls(indexUrl: String*) = this.copy(pipExtraIndexUrls = pipExtraIndexUrls ++ indexUrl)

  def withArguments(args: String*) = this.copy(arguments = arguments ++ args)

  def withInput(inputs: S3DataNode*) = this.copy(input = input ++ inputs)
  def withOutput(outputs: S3DataNode*) = this.copy(output = output ++ outputs)

  def withStdoutTo(out: String) = this.copy(stdout = Option(out))
  def withStderrTo(err: String) = this.copy(stderr = Option(err))

  private[hyperion] def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  private def scriptArguments = Seq(
    pythonScriptUri.map(Seq(_)),
    pythonScript.map(Seq(_)),
    pythonRequirements.map(Seq("-r", _)),
    pythonModule.map(Seq("-m", _)),
    pipIndexUrl.map(Seq("-i", _))
  ).flatten.flatten ++ pipExtraIndexUrls.flatMap(Seq("--extra-index-url", _)) ++ Seq("--") ++ arguments

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = scriptUri,
    scriptArgument = Option(scriptArguments),
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

object PythonActivity extends RunnableObject {

  def apply(jar: String, runsOn: Ec2Resource)(implicit hc: HyperionContext) =
    new PythonActivity(
      id = PipelineObjectId("PythonActivity"),
      runsOn = runsOn,
      scriptUri = Option(s"${hc.scriptUri}run-python.sh"),
      pythonScriptUri = None,
      pythonScript = None,
      pythonModule = None,
      pythonRequirements = None,
      pipIndexUrl = None,
      pipExtraIndexUrls = Seq(),
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
