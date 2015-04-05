package com.krux.hyperion.objects

import aws.{AdpEc2Resource, AdpJsonSerializer}
import com.krux.hyperion.HyperionContext

/**
 * EC2 resource
 */
case class Ec2Resource private (
  id: PipelineObjectId,
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

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))

  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def terminatingAfter(terminateAfter: String) = this.copy(terminateAfter = terminateAfter)
  def withRole(role: String) = this.copy(role = Some(role))
  def withResourceRole(role: String) = this.copy(resourceRole = Some(role))
  def withImageId(imageId: String) = this.copy(imageId = Some(imageId))
  def withInstanceType(instanceType: String) = this.copy(instanceType = instanceType)
  def withRegion(region: String) = this.copy(region = Some(region))
  def withSecurityGroups(securityGroups: String*) = this.copy(securityGroups = securityGroups)
  def withSecurityGroupIds(securityGroupIds: String*) = this.copy(securityGroupIds = securityGroupIds)
  def withPublicIp() = this.copy(associatePublicIpAddress = true)

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
    id = PipelineObjectId("Ec2Resource"),
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
