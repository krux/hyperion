package com.krux.hyperion.activity

import com.krux.hyperion.adt.HS3Uri
import com.krux.hyperion.common.{PipelineObjectId, BaseFields}
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.resource.{Resource, Ec2Resource}

/**
 * Activity to Copy recursively between S3 Paths.
 * This does not fail in case sourcePath does not exist
 */
case class CopyS3PathActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  sourceS3Path: HS3Uri,
  destS3Path : HS3Uri
) extends BaseShellCommandActivity {

  type Self = CopyS3PathActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

}

object CopyS3PathActivity extends RunnableObject {

  def apply(sourceS3Path: HS3Uri, destS3Path: HS3Uri)(runsOn: Resource[Ec2Resource]): CopyS3PathActivity =
    new CopyS3PathActivity(
      baseFields = BaseFields(PipelineObjectId(CopyS3PathActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(s"aws s3 cp --recursive $sourceS3Path $destS3Path "),
      sourceS3Path = sourceS3Path,
      destS3Path = destS3Path
    )

}
