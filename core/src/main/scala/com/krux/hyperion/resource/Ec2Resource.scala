package com.krux.hyperion.resource

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.{AdpHttpProxy, AdpRef, AdpEc2Resource}
import com.krux.hyperion.common.{HttpProxy, PipelineObjectId}
import com.krux.hyperion.expression.Duration
import com.krux.hyperion.parameter.{DirectValueParameter, Parameter}

/**
 * EC2 resource
 */
case class Ec2Resource private (
  id: PipelineObjectId,
  instanceType: String,
  imageId: Option[String],
  role: Option[String],
  resourceRole: Option[String],
  runAsUser: Option[String],
  keyPair: Option[String],
  region: Option[String],
  availabilityZone: Option[String],
  subnetId: Option[String],
  associatePublicIpAddress: Boolean,
  securityGroups: Seq[String],
  securityGroupIds: Seq[String],
  spotBidPrice: Option[Parameter[Double]],
  useOnDemandOnLastAttempt: Option[Boolean],
  initTimeout: Option[Parameter[Duration]],
  terminateAfter: Option[Parameter[Duration]],
  actionOnResourceFailure: Option[ActionOnResourceFailure],
  actionOnTaskFailure: Option[ActionOnTaskFailure],
  httpProxy: Option[HttpProxy]
) extends ResourceObject {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def runAsUser(user: String) = this.copy(runAsUser = Option(user))
  def terminatingAfter(terminateAfter: Parameter[Duration]) = this.copy(terminateAfter = Option(terminateAfter))
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
  def withSpotBidPrice(spotBidPrice: Parameter[Double]) = this.copy(spotBidPrice = Option(spotBidPrice))
  def withUseOnDemandOnLastAttempt(useOnDemandOnLastAttempt: Boolean) = this.copy(useOnDemandOnLastAttempt = Option(useOnDemandOnLastAttempt))
  def withInitTimeout(timeout: Parameter[Duration]) = this.copy(initTimeout = Option(timeout))
  def withHttpProxy(proxy: HttpProxy) = this.copy(httpProxy = Option(proxy))

  lazy val serialize = AdpEc2Resource(
    id = id,
    name = id.toOption,
    instanceType = Option(instanceType),
    imageId = imageId,
    role = role,
    resourceRole = resourceRole,
    runAsUser = runAsUser,
    keyPair = keyPair,
    region = region,
    availabilityZone = availabilityZone,
    subnetId = subnetId,
    associatePublicIpAddress = Option(associatePublicIpAddress.toString),
    securityGroups = Option(securityGroups),
    securityGroupIds = securityGroupIds,
    spotBidPrice = spotBidPrice.map(_.toString),
    useOnDemandOnLastAttempt = useOnDemandOnLastAttempt.map(_.toString),
    initTimeout = initTimeout.map(_.toString),
    terminateAfter = terminateAfter.map(_.toString),
    actionOnResourceFailure = actionOnResourceFailure.map(_.toString),
    actionOnTaskFailure = actionOnTaskFailure.map(_.toString),
    httpProxy = httpProxy.map(_.ref)
  )

  def ref: AdpRef[AdpEc2Resource] = AdpRef(serialize)
}

object Ec2Resource {

  def apply()(implicit hc: HyperionContext) = new Ec2Resource(
    id = PipelineObjectId(Ec2Resource.getClass),
    instanceType = hc.ec2InstanceType,
    imageId = Option(hc.ec2ImageId),
    role = Option(hc.ec2Role),
    resourceRole = Option(hc.ec2ResourceRole),
    runAsUser = None,
    keyPair = hc.ec2KeyPair,
    region = Option(hc.ec2Region),
    availabilityZone = hc.ec2AvailabilityZone,
    subnetId = hc.ec2SubnetId,
    associatePublicIpAddress = false,
    securityGroups = Seq(hc.ec2SecurityGroup),
    securityGroupIds = Seq.empty,
    spotBidPrice = None,
    useOnDemandOnLastAttempt = None,
    initTimeout = None,
    terminateAfter = hc.ec2TerminateAfter.map(DirectValueParameter[Duration]),
    actionOnResourceFailure = None,
    actionOnTaskFailure = None,
    httpProxy = None
  )

}
