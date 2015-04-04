package com.krux.hyperion.objects

import aws.{AdpEc2Resource, AdpJsonSerializer}
import com.krux.hyperion.HyperionContext

/**
 * EC2 resource
 */
case class Ec2Resource private (
  id: UniquePipelineId,
  terminateAfter: String,
  role: Option[String],
  resourceRole: Option[String],
  instanceType: String,
  region: Option[String],
  imageId: Option[String],
  securityGroups: Seq[String],
  securityGroupIds: Seq[String],
  associatePublicIpAddress: Boolean
)(
  implicit val hc: HyperionContext
) extends ResourceObject {

  def forClient(client: String) = this.copy(id = new UniquePipelineId(client))
  def terminatingAfter(terminateAfter: String) = this.copy(terminateAfter = terminateAfter)
  def withRole(role: String) = this.copy(role = Some(role))
  def withResourceRole(role: String) = this.copy(resourceRole = Some(role))
  def withImageId(imageId: String) = this.copy(imageId = Some(imageId))
  def withInstanceType(instanceType: String) = this.copy(instanceType = instanceType)
  def withRegion(region: String) = this.copy(region = Some(region))
  def withSecurityGroups(securityGroups: String*) = this.copy(securityGroups = securityGroups)
  def withSecurityGroupIds(securityGroupIds: String*) = this.copy(securityGroupIds = securityGroupIds)
  def withPublicIp() = this.copy(associatePublicIpAddress = true)

  def runJar = JarActivity(this)

  def runShell = ShellCommandActivity(runsOn = this)

  def downloadFromGoogleStorage = GoogleStorageDownloadActivity(this)

  def uploadToGoogleStorage = GoogleStorageUploadActivity(this)

  def runSql(script: String, database: Database) =
    SqlActivity(
      runsOn = this,
      database = database,
      script = script
    )

  def runCopy(input: Copyable, output: Copyable) =
    CopyActivity(
      input = input,
      output = output,
      runsOn = this
    )

  def copyIntoRedshift(input: S3DataNode, output: RedshiftDataNode, insertMode: RedshiftCopyActivity.InsertMode) =
    RedshiftCopyActivity(
      input = input,
      output = output,
      insertMode = insertMode,
      runsOn = this
    )

  def copyFromRedshift(database: RedshiftDatabase, script: String, s3Path: String) =
    RedshiftUnloadActivity(
      database = database,
      script = script,
      s3Path = s3Path,
      runsOn = this
    )

  def deleteS3Path(s3Path: String) = DeleteS3PathActivity(s3Path = s3Path, runsOn = this)

  def serialize = AdpEc2Resource(
    id = id,
    name =Some(id),
    terminateAfter = terminateAfter,
    role = role,
    resourceRole = resourceRole,
    imageId = Some(imageId.getOrElse(hc.ec2ImageId)),
    instanceType = Some(instanceType),
    region = Some(region.getOrElse(hc.region)),
    securityGroups = securityGroups match {
      case Seq() => Some(Seq(hc.ec2SecurityGroup))
      case groups => Some(groups)
    },
    securityGroupIds = securityGroupIds match {
      case Seq() => None
      case groups => Some(groups)
    },
    associatePublicIpAddress = Some(associatePublicIpAddress.toString()),
    keyPair = keyPair
  )
}

object Ec2Resource {

  def apply()(implicit hc: HyperionContext) = new Ec2Resource(
    id = new UniquePipelineId("Ec2Resource"),
    terminateAfter = hc.ec2TerminateAfter,
    role = None,
    resourceRole = None,
    instanceType = hc.ec2InstanceType,
    region = None,
    imageId = None,
    securityGroups = Seq(),
    securityGroupIds = Seq(),
    associatePublicIpAddress = false
  )

}
