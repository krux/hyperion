package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{ PipelineObject, PipelineObjectId, ObjectFields, S3Uri }
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.adt.{ HInt, HDuration, HS3Uri, HString, HBoolean, HType }
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{ Resource, Ec2Resource }

/**
 * Shell command activity that runs a given python script
 */
case class PythonActivity private (
  baseFields: ObjectFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  pythonScriptUri: Option[HS3Uri],
  pythonScript: Option[HString],
  pythonModule: Option[HString],
  pythonRequirements: Option[HString],
  pipIndexUrl: Option[HString],
  pipExtraIndexUrls: Seq[HString],
  arguments: Seq[HString]
) extends BaseShellCommandActivity with WithS3Input with WithS3Output {

  type Self = PythonActivity

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

  def withScript(pythonScript: HString) = this.copy(pythonScript = Option(pythonScript))
  def withModule(pythonModule: HString) = this.copy(pythonModule = Option(pythonModule))
  def withRequirements(pythonRequirements: HString) = this.copy(pythonRequirements = Option(pythonRequirements))
  def withIndexUrl(indexUrl: HString) = this.copy(pipIndexUrl = Option(indexUrl))
  def withExtraIndexUrls(indexUrl: HString*) = this.copy(pipExtraIndexUrls = pipExtraIndexUrls ++ indexUrl)

  override def withArguments(args: HString*) = copy(arguments = arguments ++ args)

  override def scriptArguments: Seq[HType] = Seq(
    pythonScriptUri.map(Seq(_)),
    pythonScript.map(Seq(_)),
    pythonRequirements.map(Seq[HString]("-r", _)),
    pythonModule.map(Seq[HString]("-m", _)),
    pipIndexUrl.map(Seq[HString]("-i", _))
  ).flatten.flatten ++ pipExtraIndexUrls.flatMap(Seq[HString]("--extra-index-url", _)) ++ Seq[HString]("--") ++ arguments

}

object PythonActivity extends RunnableObject {

  def apply(pythonScriptUri: HS3Uri)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): PythonActivity =
    new PythonActivity(
      baseFields = ObjectFields(PipelineObjectId(PythonActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(S3Uri(s"${hc.scriptUri}activities/run-python.sh")),
      pythonScriptUri = Option(pythonScriptUri),
      pythonScript = None,
      pythonModule = None,
      pythonRequirements = None,
      pipIndexUrl = None,
      pipExtraIndexUrls = Seq.empty,
      arguments = Seq.empty
    )

}
