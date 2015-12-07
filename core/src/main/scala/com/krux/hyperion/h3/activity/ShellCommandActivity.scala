package com.krux.hyperion.h3.activity

import shapeless._

import com.krux.hyperion.activity.Script
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.h3.common.ObjectFields
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

  def baseFieldsLens = lens[Self] >> 'baseFields
  def activityFieldsLens = lens[Self] >> 'activityFields
  def shellCommandActivityFieldsLens = lens[Self] >> 'shellCommandActivityFields

}

object ShellCommandActivity extends RunnableObject {

  def apply(script: Script)(runsOn: Resource[Ec2Resource]): ShellCommandActivity =
    new ShellCommandActivity(
      baseFields = ObjectFields(PipelineObjectId(ShellCommandActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(script)
    )

}
