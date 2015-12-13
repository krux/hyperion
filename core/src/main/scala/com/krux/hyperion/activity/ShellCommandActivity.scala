package com.krux.hyperion.activity

import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.common.{ ObjectFields, PipelineObjectId }
import com.krux.hyperion.resource.{ Resource, Ec2Resource }

/**
 * Runs a command or script
 */
case class ShellCommandActivity private (
  baseFields: ObjectFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields
) extends BaseShellCommandActivity {

  type Self = ShellCommandActivity

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

}

object ShellCommandActivity extends RunnableObject {

  def apply(script: Script)(runsOn: Resource[Ec2Resource]): ShellCommandActivity =
    new ShellCommandActivity(
      baseFields = ObjectFields(PipelineObjectId(ShellCommandActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(script)
    )

}
