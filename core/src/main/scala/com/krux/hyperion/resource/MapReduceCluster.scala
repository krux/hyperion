package com.krux.hyperion.resource

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.AdpEmrCluster
import com.krux.hyperion.common.PipelineObjectId

/**
 * Launch a map reduce cluster
 */
class MapReduceCluster private (
  val id: PipelineObjectId,
  val bootstrapAction: Seq[String],
  val amiVersion: String,
  val masterInstanceType: Option[String],
  val coreInstanceType: Option[String],
  val coreInstanceCount: Int,
  val taskInstanceType: Option[String],
  val taskInstanceCount: Int,
  val taskInstanceBidPrice: Option[Double],
  val terminateAfter: String,
  val keyPair: Option[String],
  val region: Option[String],
  val enableDebugging: Option[Boolean],
  val supportedProducts: Option[String],
  val subnetId: Option[String],
  val role: Option[String],
  val resourceRole: Option[String],
  val availabilityZone: Option[String],
  val coreInstanceBidPrice: Option[Double],
  val masterInstanceBidPrice: Option[Double],
  val useOnDemandOnLastAttempt: Option[Boolean],
  val visibleToAllUsers: Option[Boolean],
  val masterSecurityGroupId: Option[String],
  val slaveSecurityGroupId: Option[String],
  val additionalMasterSecurityGroupIds: Seq[String],
  val additionalSlaveSecurityGroupIds: Seq[String],
  val hadoopSchedulerType: Option[SchedulerType],
  val actionOnResourceFailure: Option[ActionOnResourceFailure],
  val actionOnTaskFailure: Option[ActionOnTaskFailure]
)(
  implicit val hc: HyperionContext
) extends EmrCluster {

  assert(taskInstanceCount >= 0)
  assert(coreInstanceCount >= 1)

  def copy(id: PipelineObjectId = id,
    bootstrapAction: Seq[String] = bootstrapAction,
    amiVersion: String = amiVersion,
    masterInstanceType: Option[String] = masterInstanceType,
    coreInstanceType: Option[String] = coreInstanceType,
    coreInstanceCount: Int = coreInstanceCount,
    taskInstanceType: Option[String] = taskInstanceType,
    taskInstanceCount: Int = taskInstanceCount,
    taskInstanceBidPrice: Option[Double] = taskInstanceBidPrice,
    terminateAfter: String = terminateAfter,
    keyPair: Option[String] = keyPair,
    region: Option[String] = region,
    enableDebugging: Option[Boolean] = enableDebugging,
    supportedProducts: Option[String] = supportedProducts,
    subnetId: Option[String] = subnetId,
    role: Option[String] = role,
    resourceRole: Option[String] = resourceRole,
    availabilityZone: Option[String] = availabilityZone,
    coreInstanceBidPrice: Option[Double] = coreInstanceBidPrice,
    masterInstanceBidPrice: Option[Double] = masterInstanceBidPrice,
    useOnDemandOnLastAttempt: Option[Boolean] = useOnDemandOnLastAttempt,
    visibleToAllUsers: Option[Boolean] = visibleToAllUsers,
    masterSecurityGroupId: Option[String] = masterSecurityGroupId,
    slaveSecurityGroupId: Option[String] = slaveSecurityGroupId,
    additionalMasterSecurityGroupIds: Seq[String] = additionalMasterSecurityGroupIds,
    additionalSlaveSecurityGroupIds: Seq[String] = additionalSlaveSecurityGroupIds,
    hadoopSchedulerType: Option[SchedulerType] = hadoopSchedulerType,
    actionOnResourceFailure: Option[ActionOnResourceFailure] = actionOnResourceFailure,
    actionOnTaskFailure: Option[ActionOnTaskFailure] = actionOnTaskFailure) = new MapReduceCluster(id,
      bootstrapAction, amiVersion, masterInstanceType, coreInstanceType, coreInstanceCount,
      taskInstanceType, taskInstanceCount, taskInstanceBidPrice, terminateAfter,
      keyPair, region, enableDebugging, supportedProducts, subnetId, role, resourceRole,
      availabilityZone, coreInstanceBidPrice, masterInstanceBidPrice, useOnDemandOnLastAttempt,
      visibleToAllUsers, masterSecurityGroupId, slaveSecurityGroupId, additionalMasterSecurityGroupIds,
      additionalSlaveSecurityGroupIds, hadoopSchedulerType, actionOnResourceFailure, actionOnTaskFailure)
  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withBootstrapAction(action: String*) = this.copy(bootstrapAction = bootstrapAction ++ action)
  def terminatingAfter(terminateAfter: String) = this.copy(terminateAfter = terminateAfter)
  def withAmiVersion(ver: String) = this.copy(amiVersion = ver)
  def withMasterInstanceType(instanceType: String) = this.copy(masterInstanceType = Option(instanceType))
  def withMasterInstanceBidPrice(masterInstanceBidPrice: Double) = this.copy(masterInstanceBidPrice= Option(masterInstanceBidPrice))
  def withMasterSecurityGroupId(masterSecurityGroupId: String) = this.copy(masterSecurityGroupId = Option(masterSecurityGroupId))
  def withAdditionalMasterSecurityGroupIds(securityGroupId: String*) = this.copy(additionalMasterSecurityGroupIds = additionalMasterSecurityGroupIds ++ securityGroupId)
  def withCoreInstanceType(instanceType: String) = this.copy(coreInstanceType = Option(instanceType))
  def withCoreInstanceCount(instanceCount: Int) = this.copy(coreInstanceCount = instanceCount)
  def withCoreInstanceBidPrice(coreInstanceBidPrice: Double) = this.copy(coreInstanceBidPrice = Option(coreInstanceBidPrice))
  def withTaskInstanceType(instanceType: String) = this.copy(taskInstanceType = Option(instanceType))
  def withTaskInstanceCount(instanceCount: Int) = this.copy(taskInstanceCount = instanceCount)
  def withTaskInstanceBidPrice(bid: Double) = this.copy(taskInstanceBidPrice = Option(bid))
  def withSlaveSecurityGroupId(slaveSecurityGroupId: String) = this.copy(slaveSecurityGroupId = Option(slaveSecurityGroupId))
  def withAdditionalSlaveSecurityGroupIds(securityGroupIds: String*) = this.copy(additionalSlaveSecurityGroupIds = additionalSlaveSecurityGroupIds ++ securityGroupIds)
  def withKeyPair(keyPair: String) = this.copy(keyPair = Option(keyPair))
  def withRegion(region: String) = this.copy(region = Option(region))
  def withAvailabilityZone(availabilityZone: String) = this.copy(availabilityZone = Option(availabilityZone))
  def withSubnetId(id: String) = this.copy(subnetId = Option(id))
  def withDebuggingEnabled() = this.copy(enableDebugging = Option(true))
  def withSupportedProducts(products: String) = this.copy(supportedProducts = Option(products))
  def withUseOnDemandOnLastAttempt(useOnDemandOnLastAttempt: Boolean) = this.copy(useOnDemandOnLastAttempt = Option(useOnDemandOnLastAttempt))
  def withVisibleToAllUsers(visibleToAllUsers: Boolean) = this.copy(visibleToAllUsers = Option(visibleToAllUsers))
  def withHadoopSchedulerType(hadoopSchedulerType: SchedulerType) = this.copy(hadoopSchedulerType = Option(hadoopSchedulerType))
  def withActionOnResourceFailure(actionOnResourceFailure: ActionOnResourceFailure) = this.copy(actionOnResourceFailure = Option(actionOnResourceFailure))
  def withActionOnTaskFailure(actionOnTaskFailure: ActionOnTaskFailure) = this.copy(actionOnTaskFailure = Option(actionOnTaskFailure))
  def withRole(role: String) = this.copy(role = Option(role))
  def withResourceRole(role: String) = this.copy(resourceRole = Option(role))

  lazy val instanceCount = 1 + coreInstanceCount + taskInstanceCount

  lazy val standardBootstrapAction = hc.emrEnvironmentUri.map(env => s"${hc.scriptUri}deploy-hyperion-emr-env.sh,$env").toList

  lazy val serialize = new AdpEmrCluster(
    id = id,
    name = id.toOption,
    bootstrapAction = standardBootstrapAction ++ bootstrapAction,
    amiVersion = Option(amiVersion),
    masterInstanceType = masterInstanceType,
    coreInstanceType = coreInstanceType,
    coreInstanceCount = Option(coreInstanceCount.toString),
    taskInstanceType = taskInstanceCount match {
      case 0 => None
      case _ => taskInstanceType
    },
    taskInstanceCount = taskInstanceCount match {
      case 0 => None
      case count => Option(count.toString)
    },
    taskInstanceBidPrice = taskInstanceCount match {
      case 0 => None
      case _ => taskInstanceBidPrice.map(_.toString)
    },
    terminateAfter = terminateAfter,
    keyPair = keyPair,
    region = region,
    enableDebugging = enableDebugging.map(_.toString),
    supportedProducts = supportedProducts,
    subnetId = subnetId,
    role = role,
    resourceRole = resourceRole,
    availabilityZone = availabilityZone,
    coreInstanceBidPrice = coreInstanceBidPrice,
    masterInstanceBidPrice = masterInstanceBidPrice,
    useOnDemandOnLastAttempt = useOnDemandOnLastAttempt,
    visibleToAllUsers = visibleToAllUsers,
    masterSecurityGroupId = masterSecurityGroupId,
    slaveSecurityGroupId = slaveSecurityGroupId,
    additionalMasterSecurityGroupIds = additionalMasterSecurityGroupIds match {
      case Seq() => None
      case groupIds => Option(groupIds)
    },
    additionalSlaveSecurityGroupIds = additionalSlaveSecurityGroupIds match {
      case Seq() => None
      case groupIds => Option(groupIds)
    },
    hadoopSchedulerType = hadoopSchedulerType.map(_.toString),
    actionOnResourceFailure = actionOnResourceFailure.map(_.toString),
    actionOnTaskFailure = actionOnTaskFailure.map(_.toString)
  )

}

object MapReduceCluster {
  def apply()(implicit hc: HyperionContext) = new MapReduceCluster(
    id = PipelineObjectId("MapReduceCluster"),
    bootstrapAction = Seq(),
    amiVersion = hc.emrAmiVersion,
    masterInstanceType = Option(hc.emrInstanceType),
    coreInstanceType = Option(hc.emrInstanceType),
    coreInstanceCount = 2,
    taskInstanceType = Option(hc.emrInstanceType),
    taskInstanceCount = 0,
    taskInstanceBidPrice = None,
    terminateAfter = hc.emrTerminateAfter,
    keyPair = hc.emrKeyPair,
    region = Option(hc.emrRegion),
    enableDebugging = None,
    supportedProducts = None,
    subnetId = hc.emrSubnetId,
    role = Option(hc.emrRole),
    resourceRole = Option(hc.emrResourceRole),
    availabilityZone = hc.emrAvailabilityZone,
    coreInstanceBidPrice = None,
    masterInstanceBidPrice = None,
    useOnDemandOnLastAttempt = None,
    visibleToAllUsers = None,
    masterSecurityGroupId = None,
    slaveSecurityGroupId = None,
    additionalMasterSecurityGroupIds = Seq(),
    additionalSlaveSecurityGroupIds = Seq(),
    hadoopSchedulerType = None,
    actionOnResourceFailure = None,
    actionOnTaskFailure = None
  )
}
