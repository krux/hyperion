package com.krux.hyperion.resource

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.{AdpRef, AdpEc2Resource}
import com.krux.hyperion.common.PipelineObjectId

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
  subnetId: Option[String],
  availabilityZone: Option[String],
  spotBidPrice: Option[Double],
  useOnDemandOnLastAttempt: Option[Boolean],
  actionOnResourceFailure: Option[ActionOnResourceFailure],
  actionOnTaskFailure: Option[ActionOnTaskFailure]
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
  def withActionOnResourceFailure(actionOnResourceFailure: ActionOnResourceFailure) = this.copy(actionOnResourceFailure = Option(actionOnResourceFailure))
  def withActionOnTaskFailure(actionOnTaskFailure: ActionOnTaskFailure) = this.copy(actionOnTaskFailure = Option(actionOnTaskFailure))
  def withAvailabilityZone(availabilityZone: String) = this.copy(availabilityZone = Option(availabilityZone))
  def withSpotBidPrice(spotBidPrice: Double) = this.copy(spotBidPrice = Option(spotBidPrice))
  def withUseOnDemandOnLastAttempt(useOnDemandOnLastAttempt: Boolean) = this.copy(useOnDemandOnLastAttempt = Option(useOnDemandOnLastAttempt))

  lazy val serialize = AdpEc2Resource(
    id = id,
    name = id.toOption,
    terminateAfter = terminateAfter,
    role = role,
    resourceRole = resourceRole,
    imageId = imageId,
    instanceType = Option(instanceType),
    region = region,
    securityGroups = securityGroups match {
      case Seq() => Option(Seq(hc.ec2SecurityGroup))
      case groups => Option(groups)
    },
    securityGroupIds = securityGroupIds,
    associatePublicIpAddress = Option(associatePublicIpAddress.toString),
    keyPair = keyPair,
    subnetId = subnetId,
    availabilityZone = availabilityZone,
    spotBidPrice = spotBidPrice,
    useOnDemandOnLastAttempt = useOnDemandOnLastAttempt,
    actionOnResourceFailure = actionOnResourceFailure.map(_.toString),
    actionOnTaskFailure = actionOnTaskFailure.map(_.toString)
  )

  def ref: AdpRef[AdpEc2Resource] = AdpRef(serialize)
}

object Ec2Resource {

  def apply()(implicit hc: HyperionContext) = new Ec2Resource(
    id = PipelineObjectId("Ec2Resource"),
    terminateAfter = hc.ec2TerminateAfter,
    role = Option(hc.ec2Role),
    resourceRole = Option(hc.ec2ResourceRole),
    instanceType = hc.ec2InstanceType,
    region = Option(hc.ec2Region),
    imageId = Option(hc.ec2ImageId),
    keyPair = hc.ec2KeyPair,
    securityGroups = Seq(),
    securityGroupIds = Seq(),
    associatePublicIpAddress = false,
    subnetId = hc.ec2SubnetId,
    availabilityZone = hc.ec2AvailabilityZone,
    spotBidPrice = None,
    useOnDemandOnLastAttempt = None,
    actionOnResourceFailure = None,
    actionOnTaskFailure = None
  )

}
