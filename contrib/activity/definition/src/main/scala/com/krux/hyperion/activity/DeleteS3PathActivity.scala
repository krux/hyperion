package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.{HInt, HDuration, HS3Uri, HString, HBoolean}
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObjectId, PipelineObject, BaseFields}
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

/**
 * Activity to recursively delete files in an S3 path.
 */
case class DeleteS3PathActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  s3Path: HS3Uri
) extends BaseShellCommandActivity {

  type Self = DeleteS3PathActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

}

object DeleteS3PathActivity extends RunnableObject {

  def apply(s3Path: HS3Uri)(runsOn: Resource[Ec2Resource]): DeleteS3PathActivity =
    new DeleteS3PathActivity(
      baseFields = BaseFields(PipelineObjectId(DeleteS3PathActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(s"aws s3 rm --recursive $s3Path"),
      s3Path = s3Path
    )

}
