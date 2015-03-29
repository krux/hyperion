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

  def runJar(name: String) = JarActivity(name, this)
  def runShell(name: String) = ShellCommandActivity(name, this)

  def downloadFromGoogleStorage(name: String) = GoogleStorageDownloadActivity(name, this)
  def uploadToGoogleStorage(name: String) = GoogleStorageUploadActivity(name, this)

  def runSql(name: String, script: String, database: Database) =
    SqlActivity(name, this, database, script, Seq(), Seq())

  def runCopyActivity(input: Copyable, output: Copyable) =
    CopyActivity(input, output, this)

  def serialize = AdpEc2Resource(
      id,
      Some(id),
      terminateAfter,
      role,
      resourceRole,
      imageId match {
        case None => Some(hc.ec2ImageId)
        case other => other
      },
      Some(instanceType),
      region match {
        case None => Some(hc.region)
        case other => other
      },
      securityGroups match {
        case Seq() => Some(Seq(hc.ec2SecurityGroup))
        case groups => Some(groups)
      },
      securityGroupIds match {
        case Seq() => None
        case groups => Some(groups)
      },
      Some(associatePublicIpAddress.toString()),
      keyPair
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
