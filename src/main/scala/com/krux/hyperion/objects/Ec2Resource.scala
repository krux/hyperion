package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpEc2Resource, AdpRef}
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
  keyPair: Option[String],
  securityGroups: Seq[String],
  securityGroupIds: Seq[String],
  associatePublicIpAddress: Boolean,
  subnetId: Option[String]
)(
  implicit val hc: HyperionContext
) extends ResourceObject {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def terminatingAfter(terminateAfter: String) = this.copy(terminateAfter = terminateAfter)
  def withRole(role: String) = this.copy(role = Option(role))
  def withResourceRole(role: String) = this.copy(resourceRole = Option(role))
  def withInstanceType(instanceType: String) = this.copy(instanceType = instanceType)
  def withRegion(region: String) = this.copy(region = Option(region))
  def withImageId(imageId: String) = this.copy(imageId = Option(imageId))
  def withSecurityGroups(groups: String*) = this.copy(securityGroups = securityGroups ++ groups)
  def withSecurityGroupIds(groupIds: String*) = this.copy(securityGroupIds = securityGroupIds ++ groupIds)
  def withPublicIp() = this.copy(associatePublicIpAddress = true)
  def withSubnetId(id: String) = this.copy(subnetId = Option(id))

  lazy val serialize = AdpEc2Resource(
    id = id,
    name = id.toOption,
    terminateAfter = terminateAfter,
    role = role,
    resourceRole = resourceRole,
    imageId = Option(imageId.getOrElse(hc.ec2ImageId)),
    instanceType = Option(instanceType),
    region = Option(region.getOrElse(hc.region)),
    securityGroups = securityGroups match {
      case Seq() => Option(Seq(hc.ec2SecurityGroup))
      case groups => Option(groups)
    },
    securityGroupIds = securityGroupIds,
    associatePublicIpAddress = Option(associatePublicIpAddress.toString()),
    keyPair = keyPair,
    subnetId = subnetId
  )

  def ref: AdpRef[AdpEc2Resource] = AdpRef(serialize)
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
    keyPair = hc.keyPair,
    securityGroups = Seq(),
    securityGroupIds = Seq(),
    associatePublicIpAddress = false,
    subnetId = None
  )

}
