package com.krux.hyperion.activity

import com.krux.hyperion.adt.{HS3Uri, HString}
import com.krux.hyperion.common.{BaseFields, PipelineObjectId}
import com.krux.hyperion.resource.{Ec2Resource, Resource}
import com.krux.hyperion.expression.RunnableObject

case class AwsS3CpActivity private(
                                    baseFields: BaseFields,
                                    activityFields: ActivityFields[Ec2Resource],
                                    shellCommandActivityFields: ShellCommandActivityFields,
                                    sourceS3Path: HS3Uri,
                                    destinationS3Path: HS3Uri
                                  ) extends BaseShellCommandActivity {

  type Self = AwsS3CpActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

  def useProfile(
      profile: HString,
      credentialsS3Uri: HS3Uri,
      configS3Uri: HS3Uri) = updateShellCommandActivityFields(
    shellCommandActivityFields.copy(script =
      s"""`aws s3 cp $credentialsS3Uri ~/.aws/credentials`;
          `aws s3 cp $configS3Uri ~/.aws/config`;
          `aws s3 cp --recursive --profile $profile $sourceS3Path $destinationS3Path`;
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
      shellCommandActivityFields = ShellCommandActivityFields(s"aws s3 cp --recursive $sourceS3Path $destinationS3Path"),
      sourceS3Path = sourceS3Path,
      destinationS3Path = destinationS3Path
    )
}
