package com.krux.hyperion.activity

import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.common.{ BaseFields, PipelineObjectId }
import com.krux.hyperion.resource.{ Resource, Ec2Resource }

/**
 * Runs a command or script
 */
case class ShellCommandActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields
) extends BaseShellCommandActivity with WithS3Input with WithS3Output {

  type Self = ShellCommandActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

}

object ShellCommandActivity extends RunnableObject {

  def apply(script: Script)(runsOn: Resource[Ec2Resource]): ShellCommandActivity =
    new ShellCommandActivity(
      baseFields = BaseFields(PipelineObjectId(ShellCommandActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(script)
    )

}
