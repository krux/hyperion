package com.krux.hyperion.resource

import com.krux.hyperion.adt.HType._
import com.krux.hyperion.adt.{HDuration, HDouble, HBoolean, HString}
import com.krux.hyperion.aws.{AdpRef, AdpEc2Resource}
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.HyperionContext

/**
 * EC2 resource
 */
case class Ec2Resource private (
  id: PipelineObjectId,
  instanceType: HString,
  imageId: Option[HString],
  role: Option[HString],
  resourceRole: Option[HString],
  runAsUser: Option[HString],
  keyPair: Option[HString],
  region: Option[HString],
  availabilityZone: Option[HString],
  subnetId: Option[HString],
  associatePublicIpAddress: HBoolean,
  securityGroups: Seq[HString],
  securityGroupIds: Seq[HString],
  spotBidPrice: Option[HDouble],
  useOnDemandOnLastAttempt: Option[HBoolean],
  initTimeout: Option[HDuration],
  terminateAfter: Option[HDuration],
  actionOnResourceFailure: Option[ActionOnResourceFailure],
  actionOnTaskFailure: Option[ActionOnTaskFailure]
) extends ResourceObject {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def runAsUser(user: HString) = this.copy(runAsUser = Option(user))
  def terminatingAfter(terminateAfter: HDuration) = this.copy(terminateAfter = Option(terminateAfter))
  def withRole(role: HString) = this.copy(role = Option(role))
  def withResourceRole(role: HString) = this.copy(resourceRole = Option(role))
  def withInstanceType(instanceType: HString) = this.copy(instanceType = instanceType)
  def withRegion(region: HString) = this.copy(region = Option(region))
  def withImageId(imageId: HString) = this.copy(imageId = Option(imageId))
  def withSecurityGroups(groups: HString*) = this.copy(securityGroups = securityGroups ++ groups)
  def withSecurityGroupIds(groupIds: HString*) = this.copy(securityGroupIds = securityGroupIds ++ groupIds)
  def withPublicIp() = this.copy(associatePublicIpAddress = HBoolean.True)
  def withSubnetId(id: HString) = this.copy(subnetId = Option(id))
  def withActionOnResourceFailure(actionOnResourceFailure: ActionOnResourceFailure) = this.copy(actionOnResourceFailure = Option(actionOnResourceFailure))
  def withActionOnTaskFailure(actionOnTaskFailure: ActionOnTaskFailure) = this.copy(actionOnTaskFailure = Option(actionOnTaskFailure))
  def withAvailabilityZone(availabilityZone: HString) = this.copy(availabilityZone = Option(availabilityZone))
  def withSpotBidPrice(spotBidPrice: HDouble) = this.copy(spotBidPrice = Option(spotBidPrice))
  def withUseOnDemandOnLastAttempt(useOnDemandOnLastAttempt: HBoolean) = this.copy(useOnDemandOnLastAttempt = Option(useOnDemandOnLastAttempt))
  def withInitTimeout(timeout: HDuration) = this.copy(initTimeout = Option(timeout))

  lazy val serialize = AdpEc2Resource(
    id = id,
    name = id.toOption,
    instanceType = Option(instanceType.serialize),
    imageId = imageId.map(_.serialize),
    role = role.map(_.serialize),
    resourceRole = resourceRole.map(_.serialize),
    runAsUser = runAsUser.map(_.serialize),
    keyPair = keyPair.map(_.serialize),
    region = region.map(_.serialize),
    availabilityZone = availabilityZone.map(_.serialize),
    subnetId = subnetId.map(_.serialize),
    associatePublicIpAddress = Option(associatePublicIpAddress.serialize),
    securityGroups = securityGroups.map(_.serialize),
    securityGroupIds = securityGroupIds.map(_.serialize),
    spotBidPrice = spotBidPrice.map(_.serialize),
    useOnDemandOnLastAttempt = useOnDemandOnLastAttempt.map(_.serialize),
    initTimeout = initTimeout.map(_.serialize),
    terminateAfter = terminateAfter.map(_.serialize),
    actionOnResourceFailure = actionOnResourceFailure.map(_.serialize),
    actionOnTaskFailure = actionOnTaskFailure.map(_.serialize)
  )

  def ref: AdpRef[AdpEc2Resource] = AdpRef(serialize)
}

object Ec2Resource {

  def apply()(implicit hc: HyperionContext) = new Ec2Resource(
    id = PipelineObjectId(Ec2Resource.getClass),
    instanceType = hc.ec2InstanceType,
    imageId = Option(hc.ec2ImageId: HString),
    role = Option(hc.ec2Role: HString),
    resourceRole = Option(hc.ec2ResourceRole: HString),
    runAsUser = None,
    keyPair = hc.ec2KeyPair.map(x => x: HString),
    region = Option(hc.ec2Region: HString),
    availabilityZone = hc.ec2AvailabilityZone.map(x => x: HString),
    subnetId = hc.ec2SubnetId.map(x => x: HString),
    associatePublicIpAddress = HBoolean.False,
    securityGroups = Seq(hc.ec2SecurityGroup),
    securityGroupIds = Seq.empty,
    spotBidPrice = None,
    useOnDemandOnLastAttempt = None,
    initTimeout = None,
    terminateAfter = hc.ec2TerminateAfter.map(duration2HDuration),
    actionOnResourceFailure = None,
    actionOnTaskFailure = None
  )

}
