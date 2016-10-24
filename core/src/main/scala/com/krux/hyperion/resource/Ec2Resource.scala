package com.krux.hyperion.resource

import com.krux.hyperion.adt.{ HDouble, HBoolean, HString, HDuration }
import com.krux.hyperion.aws.{ AdpRef, AdpEc2Resource }
import com.krux.hyperion.common.{ BaseFields, PipelineObjectId }
import com.krux.hyperion.HyperionContext

/**
 * EC2 resource
 */
case class Ec2Resource private (
  baseFields: BaseFields,
  resourceFields: ResourceFields,
  instanceType: HString,
  imageId: Option[HString],
  runAsUser: Option[HString],
  associatePublicIpAddress: HBoolean,
  securityGroups: Seq[HString],
  securityGroupIds: Seq[HString],
  spotBidPrice: Option[HDouble]
) extends ResourceObject {

  type Self = Ec2Resource

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateResourceFields(fields: ResourceFields) = copy(resourceFields = fields)

  def runAsUser(user: HString) = copy(runAsUser = Option(user))
  def withInstanceType(instanceType: HString) = copy(instanceType = instanceType)
  def withImageId(imageId: HString) = copy(imageId = Option(imageId))
  def withSecurityGroups(groups: HString*) = copy(securityGroups = securityGroups ++ groups)
  def withSecurityGroupIds(groupIds: HString*) = copy(securityGroupIds = securityGroupIds ++ groupIds)
  def withPublicIp() = copy(associatePublicIpAddress = HBoolean.True)
  def withSpotBidPrice(spotBidPrice: HDouble) = copy(spotBidPrice = Option(spotBidPrice))

  lazy val serialize = AdpEc2Resource(
    id = id,
    name = name,
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
    actionOnTaskFailure = actionOnTaskFailure.map(_.serialize),
    httpProxy = httpProxy.map(_.ref),
    maximumRetries = maximumRetries.map(_.serialize)
  )

  def ref: AdpRef[AdpEc2Resource] = AdpRef(serialize)
}

object Ec2Resource {

  def apply()(implicit hc: HyperionContext) = new Ec2Resource(
    baseFields = BaseFields(PipelineObjectId(Ec2Resource.getClass)),
    resourceFields = defaultResourceFields(hc),
    instanceType = hc.ec2InstanceType,
    imageId = Option(hc.ec2ImageId: HString),
    runAsUser = None,
    associatePublicIpAddress = HBoolean.False,
    securityGroups = hc.ec2SecurityGroup.toSeq,
    securityGroupIds = hc.ec2SecurityGroupId.toSeq,
    spotBidPrice = None
  )

  def defaultResourceFields(hc: HyperionContext) = ResourceFields(
    role = Option(hc.ec2Role: HString),
    resourceRole = Option(hc.ec2ResourceRole: HString),
    keyPair = hc.ec2KeyPair.map(x => x: HString),
    region = Option(hc.ec2Region: HString),
    availabilityZone = hc.ec2AvailabilityZone.map(x => x: HString),
    subnetId = hc.ec2SubnetId.map(x => x: HString),
    terminateAfter = hc.ec2TerminateAfter.map(x => x: HDuration)
  )

}
