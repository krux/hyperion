package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.adt.{ HS3Uri, HString, HType }
import com.krux.hyperion.common.{ BaseFields, PipelineObjectId, S3Uri }
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.resource.{ Ec2Resource, Resource }

/**
 * Shell command activity that runs a given python script
 */
case class PythonActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  pythonScriptUri: Option[HS3Uri],
  pythonScript: Option[HString],
  pythonModule: Option[HString],
  pythonRequirements: Option[HString],
  pipIndexUrl: Option[HString],
  pipExtraIndexUrls: Seq[HString]
) extends BaseShellCommandActivity with WithS3Input with WithS3Output {

  type Self = PythonActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

  def withScript(pythonScript: HString) = copy(pythonScript = Option(pythonScript))
  def withModule(pythonModule: HString) = copy(pythonModule = Option(pythonModule))
  def withRequirements(pythonRequirements: HString) = copy(pythonRequirements = Option(pythonRequirements))
  def withIndexUrl(indexUrl: HString) = copy(pipIndexUrl = Option(indexUrl))
  def withExtraIndexUrls(indexUrl: HString*) = copy(pipExtraIndexUrls = pipExtraIndexUrls ++ indexUrl)

  override def scriptArguments: Seq[HType] = Seq(
    pythonScriptUri.map(Seq(_)),
    pythonScript.map(Seq(_)),
    pythonRequirements.map(Seq[HString]("-r", _)),
    pythonModule.map(Seq[HString]("-m", _)),
    pipIndexUrl.map(Seq[HString]("-i", _))
  ).flatten.flatten ++ pipExtraIndexUrls.flatMap(Seq[HString]("--extra-index-url", _)) ++ Seq[HString]("--") ++ shellCommandActivityFields.scriptArguments

}

object PythonActivity extends RunnableObject {

  def apply(pythonScriptUri: HS3Uri)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): PythonActivity =
    new PythonActivity(
      baseFields = BaseFields(PipelineObjectId(PythonActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(S3Uri(s"${hc.scriptUri}activities/run-python.sh")),
      pythonScriptUri = Option(pythonScriptUri),
      pythonScript = None,
      pythonModule = None,
      pythonRequirements = None,
      pipIndexUrl = None,
      pipExtraIndexUrls = Seq.empty
    )

}
