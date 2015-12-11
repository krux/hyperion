package com.krux.hyperion.h3.activity

import com.krux.hyperion.adt.{ HS3Uri, HString }
import com.krux.hyperion.common.S3Uri
import com.krux.hyperion.h3.common.{ PipelineObjectId, ObjectFields }
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.h3.resource.{Resource, Ec2Resource}

/**
 * Shell command activity that runs a given Jar
 */
case class JarActivity private (
  baseFields: ObjectFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  jarUri: HS3Uri,
  mainClass: Option[MainClass],
  arguments: Seq[HString]
) extends BaseShellCommandActivity {

  type Self = JarActivity

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

  assert(script.uri.nonEmpty)

  def withMainClass(mainClass: MainClass) = this.copy(mainClass = Option(mainClass))

  /**
   * The arguments passed to the script (if any)
   * TODO write unit test to make sure super act as intended
   */
  def withOptions(opts: HString*) = super.withArguments(opts: _*)
  def options = shellCommandActivityFields.scriptArguments

  /**
   * Arguments passed to the jar
   */
  override def withArguments(args: HString*) = this.copy(arguments = arguments ++ args)

  override def scriptArguments =
    (jarUri.serialize: HString) +: options ++: (mainClass.fullName: HString) +: arguments

  // def objects: Iterable[PipelineObject] = runsOn.toSeq ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

}

object JarActivity extends RunnableObject {

  def apply(jarUri: HS3Uri)(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): JarActivity =
    new JarActivity(
      baseFields = ObjectFields(PipelineObjectId(ShellCommandActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(S3Uri(s"${hc.scriptUri}activities/run-jar.sh")),
      jarUri = jarUri,
      mainClass = None,
      arguments = Seq.empty
    )

}
