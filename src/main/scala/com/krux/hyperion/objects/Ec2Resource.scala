package com.krux.hyperion.objects

import aws.{AdpEc2Resource, AdpJsonSerializer}
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.util.PipelineId

/**
 * EC2 resource
 */
case class Ec2Resource(
  id: String,
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

  def forClient(client: String) = this.copy(id = s"${id}_${client}")
  def terminatingAfter(terminateAfter: String) = this.copy(terminateAfter = terminateAfter)
  def withRole(role: String) = this.copy(role = Some(role))
  def withResourceRole(role: String) = this.copy(resourceRole = Some(role))
  def withImageId(imageId: String) = this.copy(imageId = Some(imageId))
  def withInstanceType(instanceType: String) = this.copy(instanceType = instanceType)
  def withRegion(region: String) = this.copy(region = Some(region))
  def withSecurityGroups(securityGroups: String*) = this.copy(securityGroups = securityGroups)
  def withSecurityGroupIds(securityGroupIds: String*) = this.copy(securityGroupIds = securityGroupIds)
  def withPublicIp() = this.copy(associatePublicIpAddress = true)

  def runJar(name: String) =
    JarActivity(
      id = name,
      runsOn = this
    )

  def runShell(name: String) =
    ShellCommandActivity(
      id = name,
      runsOn = this
    )

  def downloadFromGoogleStorage(name: String) =
    GoogleStorageDownloadActivity(
      id = name,
      runsOn = this
    )

  def uploadToGoogleStorage(name: String) =
    GoogleStorageUploadActivity(
      id = name,
      runsOn = this
    )

  def runSql(name: String, script: String, database: Database) =
    SqlActivity(
      id = name,
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

  def copyIntoRedshift(id: String, input: S3DataNode, output: RedshiftDataNode, insertMode: RedshiftCopyActivity.InsertMode) =
    RedshiftCopyActivity(
      id = id,
      input = input,
      output = output,
      insertMode = insertMode,
      runsOn = this
    )

  def copyFromRedshift(id: String, database: RedshiftDatabase, script: String, s3Path: String) =
    RedshiftUnloadActivity(
      id = id,
      database = database,
      script = script,
      s3Path = s3Path,
      runsOn = this
    )

  def deleteS3Path(id: String, s3Path: String) =
    DeleteS3PathActivity(
      id = id,
      s3Path = s3Path,
      runsOn = this
    )

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
    id = "Ec2Resource",
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
