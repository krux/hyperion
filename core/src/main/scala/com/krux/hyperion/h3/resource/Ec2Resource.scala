package com.krux.hyperion.h3.resource

import com.krux.hyperion.adt.HType._
import com.krux.hyperion.adt.{ HDouble, HBoolean, HString }
import com.krux.hyperion.aws.{ AdpRef, AdpEc2Resource }
import com.krux.hyperion.h3.common.{ ObjectFields, PipelineObjectId }
import com.krux.hyperion.HyperionContext

/**
 * EC2 resource
 */
case class Ec2Resource private (
  baseFields: ObjectFields,
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

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateResourceFields(fields: ResourceFields) = copy(resourceFields = fields)

  def runAsUser(user: HString) = this.copy(runAsUser = Option(user))
  def withInstanceType(instanceType: HString) = this.copy(instanceType = instanceType)
  def withImageId(imageId: HString) = this.copy(imageId = Option(imageId))
  def withSecurityGroups(groups: HString*) = this.copy(securityGroups = securityGroups ++ groups)
  def withSecurityGroupIds(groupIds: HString*) = this.copy(securityGroupIds = securityGroupIds ++ groupIds)
  def withPublicIp() = this.copy(associatePublicIpAddress = HBoolean.True)
  def withSpotBidPrice(spotBidPrice: HDouble) = this.copy(spotBidPrice = Option(spotBidPrice))

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
    actionOnTaskFailure = actionOnTaskFailure.map(_.serialize),
    httpProxy = httpProxy.map(_.ref)
  )

  def ref: AdpRef[AdpEc2Resource] = AdpRef(serialize)
}

object Ec2Resource {

  def apply()(implicit hc: HyperionContext) = new Ec2Resource(
    baseFields = ObjectFields(PipelineObjectId(Ec2Resource.getClass)),
    resourceFields = defaultResourceFields(hc),
    instanceType = hc.ec2InstanceType,
    imageId = Option(hc.ec2ImageId: HString),
    runAsUser = None,
    associatePublicIpAddress = HBoolean.False,
    securityGroups = Seq(hc.ec2SecurityGroup),
    securityGroupIds = Seq.empty,
    spotBidPrice = None
  )

  def defaultResourceFields(hc: HyperionContext) = ResourceFields(
    role = Option(hc.ec2Role: HString),
    resourceRole = Option(hc.ec2ResourceRole: HString),
    keyPair = hc.ec2KeyPair.map(x => x: HString),
    region = Option(hc.ec2Region: HString),
    availabilityZone = hc.ec2AvailabilityZone.map(x => x: HString),
    subnetId = hc.ec2SubnetId.map(x => x: HString)
  )

}
