package com.krux.hyperion.resource

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.aws.AdpEmrCluster
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.expression.Duration
import com.krux.hyperion.parameter.{DirectValueParameter, Parameter}

/**
 * Launch a map reduce cluster
 */
class MapReduceCluster private (
  val id: PipelineObjectId,
  val amiVersion: String,
  val supportedProducts: Option[String],
  val standardBootstrapAction: Seq[String],
  val bootstrapAction: Seq[String],
  val enableDebugging: Option[Boolean],
  val hadoopSchedulerType: Option[SchedulerType],
  val keyPair: Option[String],
  val masterInstanceBidPrice: Option[Parameter[Double]],
  val masterInstanceType: Option[String],
  val coreInstanceBidPrice: Option[Parameter[Double]],
  val coreInstanceCount: Parameter[Int],
  val coreInstanceType: Option[String],
  val taskInstanceBidPrice: Option[Parameter[Double]],
  val taskInstanceCount: Parameter[Int],
  val taskInstanceType: Option[String],
  val region: Option[String],
  val availabilityZone: Option[String],
  val resourceRole: Option[String],
  val role: Option[String],
  val subnetId: Option[String],
  val masterSecurityGroupId: Option[String],
  val additionalMasterSecurityGroupIds: Seq[String],
  val slaveSecurityGroupId: Option[String],
  val additionalSlaveSecurityGroupIds: Seq[String],
  val useOnDemandOnLastAttempt: Option[Boolean],
  val visibleToAllUsers: Option[Boolean],
  val initTimeout: Option[Parameter[Duration]],
  val terminateAfter: Option[Parameter[Duration]],
  val actionOnResourceFailure: Option[ActionOnResourceFailure],
  val actionOnTaskFailure: Option[ActionOnTaskFailure]
) extends EmrCluster {

  assert(taskInstanceCount.value >= 0)
  assert(coreInstanceCount.value >= 1)

  def copy(id: PipelineObjectId = id,
    amiVersion: String = amiVersion,
    supportedProducts: Option[String] = supportedProducts,
    standardBootstrapAction: Seq[String] = standardBootstrapAction,
    bootstrapAction: Seq[String] = bootstrapAction,
    enableDebugging: Option[Boolean] = enableDebugging,
    hadoopSchedulerType: Option[SchedulerType] = hadoopSchedulerType,
    keyPair: Option[String] = keyPair,
    masterInstanceBidPrice: Option[Parameter[Double]] = masterInstanceBidPrice,
    masterInstanceType: Option[String] = masterInstanceType,
    coreInstanceBidPrice: Option[Parameter[Double]] = coreInstanceBidPrice,
    coreInstanceCount: Parameter[Int] = coreInstanceCount,
    coreInstanceType: Option[String] = coreInstanceType,
    taskInstanceBidPrice: Option[Parameter[Double]] = taskInstanceBidPrice,
    taskInstanceCount: Parameter[Int] = taskInstanceCount,
    taskInstanceType: Option[String] = taskInstanceType,
    region: Option[String] = region,
    availabilityZone: Option[String] = availabilityZone,
    resourceRole: Option[String] = resourceRole,
    role: Option[String] = role,
    subnetId: Option[String] = subnetId,
    masterSecurityGroupId: Option[String] = masterSecurityGroupId,
    additionalMasterSecurityGroupIds: Seq[String] = additionalMasterSecurityGroupIds,
    slaveSecurityGroupId: Option[String] = slaveSecurityGroupId,
    additionalSlaveSecurityGroupIds: Seq[String] = additionalSlaveSecurityGroupIds,
    useOnDemandOnLastAttempt: Option[Boolean] = useOnDemandOnLastAttempt,
    visibleToAllUsers: Option[Boolean] = visibleToAllUsers,
    initTimeout: Option[Parameter[Duration]] = initTimeout,
    terminateAfter: Option[Parameter[Duration]] = terminateAfter,
    actionOnResourceFailure: Option[ActionOnResourceFailure] = actionOnResourceFailure,
    actionOnTaskFailure: Option[ActionOnTaskFailure] = actionOnTaskFailure
  ) = new MapReduceCluster(id, amiVersion, supportedProducts, standardBootstrapAction, bootstrapAction, enableDebugging,
    hadoopSchedulerType, keyPair, masterInstanceBidPrice, masterInstanceType,
    coreInstanceBidPrice, coreInstanceCount, coreInstanceType,
    taskInstanceBidPrice, taskInstanceCount, taskInstanceType, region, availabilityZone, resourceRole, role, subnetId,
    masterSecurityGroupId, additionalMasterSecurityGroupIds, slaveSecurityGroupId, additionalSlaveSecurityGroupIds,
    useOnDemandOnLastAttempt, visibleToAllUsers, initTimeout, terminateAfter,
    actionOnResourceFailure, actionOnTaskFailure)

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withAmiVersion(ver: String) = this.copy(amiVersion = ver)
  def withSupportedProducts(products: String) = this.copy(supportedProducts = Option(products))
  def withBootstrapAction(action: String*) = this.copy(bootstrapAction = bootstrapAction ++ action)
  def withDebuggingEnabled() = this.copy(enableDebugging = Option(true))
  def withHadoopSchedulerType(hadoopSchedulerType: SchedulerType) = this.copy(hadoopSchedulerType = Option(hadoopSchedulerType))
  def withKeyPair(keyPair: String) = this.copy(keyPair = Option(keyPair))
  def withMasterInstanceBidPrice(masterInstanceBidPrice: Parameter[Double]) = this.copy(masterInstanceBidPrice= Option(masterInstanceBidPrice))
  def withMasterInstanceType(instanceType: String) = this.copy(masterInstanceType = Option(instanceType))
  def withCoreInstanceBidPrice(coreInstanceBidPrice: Parameter[Double]) = this.copy(coreInstanceBidPrice = Option(coreInstanceBidPrice))
  def withCoreInstanceCount(instanceCount: Parameter[Int]) = this.copy(coreInstanceCount = instanceCount)
  def withCoreInstanceType(instanceType: String) = this.copy(coreInstanceType = Option(instanceType))
  def withTaskInstanceBidPrice(bid: Parameter[Double]) = this.copy(taskInstanceBidPrice = Option(bid))
  def withTaskInstanceCount(instanceCount: Parameter[Int]) = this.copy(taskInstanceCount = instanceCount)
  def withTaskInstanceType(instanceType: String) = this.copy(taskInstanceType = Option(instanceType))
  def withRegion(region: String) = this.copy(region = Option(region))
  def withAvailabilityZone(availabilityZone: String) = this.copy(availabilityZone = Option(availabilityZone))
  def withResourceRole(role: String) = this.copy(resourceRole = Option(role))
  def withRole(role: String) = this.copy(role = Option(role))
  def withSubnetId(id: String) = this.copy(subnetId = Option(id))
  def withMasterSecurityGroupId(masterSecurityGroupId: String) = this.copy(masterSecurityGroupId = Option(masterSecurityGroupId))
  def withAdditionalMasterSecurityGroupIds(securityGroupId: String*) = this.copy(additionalMasterSecurityGroupIds = additionalMasterSecurityGroupIds ++ securityGroupId)
  def withSlaveSecurityGroupId(slaveSecurityGroupId: String) = this.copy(slaveSecurityGroupId = Option(slaveSecurityGroupId))
  def withAdditionalSlaveSecurityGroupIds(securityGroupIds: String*) = this.copy(additionalSlaveSecurityGroupIds = additionalSlaveSecurityGroupIds ++ securityGroupIds)
  def withUseOnDemandOnLastAttempt(useOnDemandOnLastAttempt: Boolean) = this.copy(useOnDemandOnLastAttempt = Option(useOnDemandOnLastAttempt))
  def withVisibleToAllUsers(visibleToAllUsers: Boolean) = this.copy(visibleToAllUsers = Option(visibleToAllUsers))
  def withInitTimeout(timeout: Parameter[Duration]) = this.copy(initTimeout = Option(timeout))
  def terminatingAfter(terminateAfter: Parameter[Duration]) = this.copy(terminateAfter = Option(terminateAfter))
  def withActionOnResourceFailure(actionOnResourceFailure: ActionOnResourceFailure) = this.copy(actionOnResourceFailure = Option(actionOnResourceFailure))
  def withActionOnTaskFailure(actionOnTaskFailure: ActionOnTaskFailure) = this.copy(actionOnTaskFailure = Option(actionOnTaskFailure))

  lazy val instanceCount = 1 + coreInstanceCount.value + taskInstanceCount.value

  lazy val serialize = new AdpEmrCluster(
    id = id,
    name = id.toOption,
    amiVersion = Option(amiVersion),
    supportedProducts = supportedProducts,
    bootstrapAction = standardBootstrapAction ++ bootstrapAction,
    enableDebugging = enableDebugging.map(_.toString),
    hadoopSchedulerType = hadoopSchedulerType.map(_.toString),
    keyPair = keyPair,
    masterInstanceBidPrice = masterInstanceBidPrice.map(_.toString),
    masterInstanceType = masterInstanceType,
    coreInstanceBidPrice = coreInstanceBidPrice.map(_.toString),
    coreInstanceCount = Option(coreInstanceCount.toString),
    coreInstanceType = coreInstanceType,
    taskInstanceBidPrice = taskInstanceCount.value match {
      case 0 => None
      case _ => taskInstanceBidPrice.map(_.toString)
    },
    taskInstanceCount = taskInstanceCount.value match {
      case 0 => None
      case count => Option(count.toString)
    },
    taskInstanceType = taskInstanceCount.value match {
      case 0 => None
      case _ => taskInstanceType
    },
    region = region,
    availabilityZone = availabilityZone,
    resourceRole = resourceRole,
    role = role,
    subnetId = subnetId,
    masterSecurityGroupId = masterSecurityGroupId,
    additionalMasterSecurityGroupIds = additionalMasterSecurityGroupIds match {
      case Seq() => None
      case groupIds => Option(groupIds)
    },
    slaveSecurityGroupId = slaveSecurityGroupId,
    additionalSlaveSecurityGroupIds = additionalSlaveSecurityGroupIds match {
      case Seq() => None
      case groupIds => Option(groupIds)
    },
    useOnDemandOnLastAttempt = useOnDemandOnLastAttempt.map(_.toString),
    visibleToAllUsers = visibleToAllUsers.map(_.toString),
    initTimeout = initTimeout.map(_.toString),
    terminateAfter = terminateAfter.map(_.toString),
    actionOnResourceFailure = actionOnResourceFailure.map(_.toString),
    actionOnTaskFailure = actionOnTaskFailure.map(_.toString)
  )

}

object MapReduceCluster {
  def apply()(implicit hc: HyperionContext) = new MapReduceCluster(
    id = PipelineObjectId(MapReduceCluster.getClass),
    amiVersion = hc.emrAmiVersion,
    supportedProducts = None,
    standardBootstrapAction = hc.emrEnvironmentUri.map(env => s"${hc.scriptUri}deploy-hyperion-emr-env.sh,$env").toList,
    bootstrapAction = Seq.empty,
    enableDebugging = None,
    hadoopSchedulerType = None,
    keyPair = hc.emrKeyPair,
    masterInstanceBidPrice = None,
    masterInstanceType = Option(hc.emrInstanceType),
    coreInstanceBidPrice = None,
    coreInstanceCount = 2,
    coreInstanceType = Option(hc.emrInstanceType),
    taskInstanceBidPrice = None,
    taskInstanceCount = 0,
    taskInstanceType = Option(hc.emrInstanceType),
    region = Option(hc.emrRegion),
    availabilityZone = hc.emrAvailabilityZone,
    resourceRole = Option(hc.emrResourceRole),
    role = Option(hc.emrRole),
    subnetId = hc.emrSubnetId,
    masterSecurityGroupId = None,
    additionalMasterSecurityGroupIds = Seq.empty,
    slaveSecurityGroupId = None,
    additionalSlaveSecurityGroupIds = Seq.empty,
    useOnDemandOnLastAttempt = None,
    visibleToAllUsers = None,
    initTimeout = None,
    terminateAfter = hc.emrTerminateAfter.map(DirectValueParameter[Duration]),
    actionOnResourceFailure = None,
    actionOnTaskFailure = None
  )
}
