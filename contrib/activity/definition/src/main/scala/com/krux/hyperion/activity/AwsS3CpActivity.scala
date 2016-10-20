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
  profile: Option[HString],
  credentialsSource: Option[HString]
) extends BaseShellCommandActivity {

  type Self = AwsS3CpActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

  def isRecursive() = copy(recursive = HBoolean.True)
  def withProfile(profile: HString) = copy(profile = Option(profile))
  def useCredentialsSource(credentialsSource: HString) = copy(credentialsSource = Option(credentialsSource))

  def build() = updateShellCommandActivityFields(
    shellCommandActivityFields.copy(script =
      s"""ARGS=""
          if ${!credentialsSource.isEmpty} ; then
            aws s3 sync ${credentialsSource.getOrElse("")} ~/.aws/;
            ARGS="$$ARGS --profile ${profile.getOrElse("default")}";
          fi
          if $recursive ; then
            ARGS="$$ARGS --recursive";
          fi
          aws s3 cp $$ARGS $sourceS3Path $destinationS3Path;
        """
    )
  )
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
      profile = None,
      credentialsSource = None
    )
}
