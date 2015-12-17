package com.krux.hyperion.activity

import com.krux.hyperion.adt.{ HS3Uri, HString }
import com.krux.hyperion.common.S3Uri
import com.krux.hyperion.common.{ PipelineObjectId, BaseFields }
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.resource.{Resource, Ec2Resource}

/**
 * Shell command activity that runs a given Jar
 */
case class JarActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  jarUri: HS3Uri,
  mainClass: Option[MainClass],
  options: Seq[HString]
) extends BaseShellCommandActivity with WithS3Input with WithS3Output {

  type Self = JarActivity

  assert(script.uri.nonEmpty)

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

  def withMainClass(mainClass: MainClass) = copy(mainClass = Option(mainClass))

  def withOptions(opts: HString*) = copy(options = options ++ opts)

  override def scriptArguments =
    (jarUri.serialize: HString) +: options ++: (mainClass.fullName: HString) +: shellCommandActivityFields.scriptArguments

}

object JarActivity extends RunnableObject {

  def apply(jarUri: HS3Uri)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): JarActivity =
    new JarActivity(
      baseFields = BaseFields(PipelineObjectId(JarActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(S3Uri(s"${hc.scriptUri}activities/run-jar.sh")),
      jarUri = jarUri,
      mainClass = None,
      options = Seq.empty
    )

}
