package com.krux.hyperion.resource

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.{AdpRef, AdpEc2Resource}
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.parameter.ParameterOf

/**
 * EC2 resource
 */
case class Ec2Resource private (
  id: PipelineObjectId,
  terminateAfter: ParameterOf[String],
  role: Option[ParameterOf[String]],
  resourceRole: Option[ParameterOf[String]],
  instanceType: ParameterOf[String],
  region: Option[ParameterOf[String]],
  imageId: Option[ParameterOf[String]],
  keyPair: Option[ParameterOf[String]],
  securityGroups: Seq[ParameterOf[String]],
  securityGroupIds: Seq[ParameterOf[String]],
  associatePublicIpAddress: Boolean,
  subnetId: Option[ParameterOf[String]],
  availabilityZone: Option[ParameterOf[String]],
  spotBidPrice: Option[ParameterOf[Double]],
  useOnDemandOnLastAttempt: Option[Boolean],
  actionOnResourceFailure: Option[ActionOnResourceFailure],
  actionOnTaskFailure: Option[ActionOnTaskFailure]
)(
  implicit val hc: HyperionContext
) extends ResourceObject {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def terminatingAfter(terminateAfter: ParameterOf[String]) = this.copy(terminateAfter = terminateAfter)
  def withRole(role: ParameterOf[String]) = this.copy(role = Option(role))
  def withResourceRole(role: ParameterOf[String]) = this.copy(resourceRole = Option(role))
  def withInstanceType(instanceType: ParameterOf[String]) = this.copy(instanceType = instanceType)
  def withRegion(region: ParameterOf[String]) = this.copy(region = Option(region))
  def withImageId(imageId: ParameterOf[String]) = this.copy(imageId = Option(imageId))
  def withSecurityGroups(groups: ParameterOf[String]*) = this.copy(securityGroups = securityGroups ++ groups)
  def withSecurityGroupIds(groupIds: ParameterOf[String]*) = this.copy(securityGroupIds = securityGroupIds ++ groupIds)
  def withPublicIp() = this.copy(associatePublicIpAddress = true)
  def withSubnetId(id: ParameterOf[String]) = this.copy(subnetId = Option(id))
  def withActionOnResourceFailure(actionOnResourceFailure: ActionOnResourceFailure) = this.copy(actionOnResourceFailure = Option(actionOnResourceFailure))
  def withActionOnTaskFailure(actionOnTaskFailure: ActionOnTaskFailure) = this.copy(actionOnTaskFailure = Option(actionOnTaskFailure))
  def withAvailabilityZone(availabilityZone: ParameterOf[String]) = this.copy(availabilityZone = Option(availabilityZone))
  def withSpotBidPrice(spotBidPrice: ParameterOf[Double]) = this.copy(spotBidPrice = Option(spotBidPrice))
  def withUseOnDemandOnLastAttempt(useOnDemandOnLastAttempt: Boolean) = this.copy(useOnDemandOnLastAttempt = Option(useOnDemandOnLastAttempt))

  lazy val serialize = AdpEc2Resource(
    id = id,
    name = id.toOption,
    terminateAfter = terminateAfter,
    role = role,
    resourceRole = resourceRole,
    imageId = imageId,
    instanceType = Option(instanceType.toString),
    region = region,
    securityGroups = securityGroups match {
      case Seq() => Option(Seq(hc.ec2SecurityGroup))
      case groups => Option(groups.map(_.toString))
    },
    securityGroupIds = Option(securityGroupIds.map(_.toString)),
    associatePublicIpAddress = Option(associatePublicIpAddress.toString),
    keyPair = keyPair,
    subnetId = subnetId,
    availabilityZone = availabilityZone,
    spotBidPrice = spotBidPrice,
    useOnDemandOnLastAttempt = useOnDemandOnLastAttempt.map(_.toString),
    actionOnResourceFailure = actionOnResourceFailure.map(_.toString),
    actionOnTaskFailure = actionOnTaskFailure.map(_.toString)
  )

  def ref: AdpRef[AdpEc2Resource] = AdpRef(serialize)
}

object Ec2Resource {

  def apply()(implicit hc: HyperionContext) = new Ec2Resource(
    id = PipelineObjectId("Ec2Resource"),
    terminateAfter = hc.ec2TerminateAfter,
    role = Option(ParameterOf.stringValue(hc.role)),
    resourceRole = Option(ParameterOf.stringValue(hc.resourceRole)),
    instanceType = hc.ec2InstanceType,
    region = Option(ParameterOf.stringValue(hc.region)),
    imageId = Option(ParameterOf.stringValue(hc.ec2ImageId)),
    keyPair = hc.keyPair,
    securityGroups = Seq(),
    securityGroupIds = Seq(),
    associatePublicIpAddress = false,
    subnetId = None,
    availabilityZone = hc.availabilityZone,
    spotBidPrice = None,
    useOnDemandOnLastAttempt = None,
    actionOnResourceFailure = None,
    actionOnTaskFailure = None
  )

}
