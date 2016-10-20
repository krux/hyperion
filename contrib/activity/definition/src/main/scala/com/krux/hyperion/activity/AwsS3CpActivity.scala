package com.krux.hyperion.activity

import com.krux.hyperion.adt.{HBoolean, HS3Uri, HString}
import com.krux.hyperion.common.{BaseFields, PipelineObjectId}
import com.krux.hyperion.resource.{Ec2Resource, Resource}
import com.krux.hyperion.expression.RunnableObject

case class AwsS3CpActivity private(
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  sourceS3Path: HS3Uri,
  destinationS3Path: HS3Uri,
  isRecursive: HBoolean,
  isOverwrite: HBoolean,
  additionalArguments: Seq[HString]
) extends BaseShellCommandActivity with WithS3Input {

  type Self = AwsS3CpActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

  def withOverwrite() = copy(isOverwrite = HBoolean.True)
  def withRecursive() = addToAdditionArguments(Seq[HString]("--recursive"))
  def withProfile(profile: HString) =  addToAdditionArguments(Seq[HString]("--profile", profile))
  def withAcl(cannedAcl: HString) = addToAdditionArguments(Seq[HString]("--acl",cannedAcl))
  def withExclude(pattern: HString) = addToAdditionArguments(Seq[HString]("--exclude", pattern))
  def withInclude(pattern: HString) = addToAdditionArguments(Seq[HString]("--include", pattern))
  def withSourceRegion(sourceRegion: HString) = addToAdditionArguments(Seq[HString]("--source-region", sourceRegion))
  def withDestinationRegion(destinationRegion: HString) = addToAdditionArguments(Seq[HString]("--region", destinationRegion))
  def withGrant(permission: HString, granteeTypeAndId: Seq[(String, String)]) = {
    addToAdditionArguments(
      Seq[HString]("--grants",
        Seq(permission, granteeTypeAndId.map { tuple => tuple.productIterator.mkString("=") }.mkString(",")).mkString("=")
      )
    )
  }
  def withAdditionalArguments(arguments: Seq[HString]) = copy(additionalArguments = this.additionalArguments ++ arguments)

  private def addToAdditionArguments(argument: Seq[HString]) = {
    copy(additionalArguments = this.additionalArguments ++ argument)
  }

  private val removeScript = if (isOverwrite) s"aws s3 rm --recursive $destinationS3Path;" else ""

  private val s3CpScript = "aws s3 cp %s %s %s;" format (additionalArguments.mkString(" "), sourceS3Path, destinationS3Path)

  override def script = {
    s"""
          if [ -n "$${INPUT1_STAGING_DIR}" ]; then
            cat $${INPUT1_STAGING_DIR}/credentials >> ~/.aws/credentials;
            cat $${INPUT1_STAGING_DIR}/config >> ~/.aws/config;
          fi
          $removeScript
          $s3CpScript
    """
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
      isRecursive = HBoolean.False,
      isOverwrite = HBoolean.False,
      additionalArguments = Seq.empty
    )
}
