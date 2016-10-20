package com.krux.hyperion.activity

import com.krux.hyperion.adt.{HBoolean, HS3Uri, HString, HType}
import com.krux.hyperion.common.{BaseFields, PipelineObjectId}
import com.krux.hyperion.resource.{Ec2Resource, Resource}
import com.krux.hyperion.expression.RunnableObject

case class AwsS3CpActivity private(
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  sourceS3Path: HS3Uri,
  destinationS3Path: HS3Uri,
  recursive: HBoolean,
  profile: Option[HString]
) extends BaseShellCommandActivity with WithS3Input {

  type Self = AwsS3CpActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

  def makeRecursive() = copy(recursive = HBoolean.True).build()
  def withProfile(profile: HString) = copy(profile = Option(profile)).build()

  private var s3CpCommand: String = "aws s3 cp"

  private def build() = {
    if (recursive) {
      s3CpCommand = "%s %s" format (s3CpCommand, "--recursive")
    }
    if (!profile.isEmpty) {
      s3CpCommand = "%s %s" format (s3CpCommand, s"--profile ${profile.getOrElse("default")}")
    }
    updateShellCommandActivityFields(
      shellCommandActivityFields.copy(script =
        s"""
          if [ -n "$${INPUT1_STAGING_DIR}" ]; then
            cp -R $${INPUT1_STAGING_DIR} ~/.aws
          fi
          $s3CpCommand $sourceS3Path $destinationS3Path;
        """
      )
    )
  }
}

object AwsS3CpActivity extends RunnableObject {

  def apply(
    sourceS3Path: HS3Uri,
    destinationS3Path: HS3Uri
  )(runsOn: Resource[Ec2Resource]): AwsS3CpActivity =

    new AwsS3CpActivity(
      baseFields = BaseFields(PipelineObjectId(AwsS3CpActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(s"aws s3 cp $sourceS3Path $destinationS3Path"),
      sourceS3Path = sourceS3Path,
      destinationS3Path = destinationS3Path,
      recursive = HBoolean.False,
      profile = None
    )
}
