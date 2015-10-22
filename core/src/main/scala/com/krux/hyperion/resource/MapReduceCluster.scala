package com.krux.hyperion.resource

import com.krux.hyperion.aws.AdpEmrCluster
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.adt.HType._
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.adt.{HInt, HDuration, HDouble, HString}

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
  val masterInstanceBidPrice: Option[HDouble],
  val masterInstanceType: Option[HString],
  val coreInstanceBidPrice: Option[HDouble],
  val coreInstanceCount: HInt,
  val coreInstanceType: Option[HString],
  val taskInstanceBidPrice: Option[HDouble],
  val taskInstanceCount: HInt,
  val taskInstanceType: Option[HString],
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
  val initTimeout: Option[HDuration],
  val terminateAfter: Option[HDuration],
  val actionOnResourceFailure: Option[ActionOnResourceFailure],
  val actionOnTaskFailure: Option[ActionOnTaskFailure]
) extends EmrCluster {

  assert((taskInstanceCount >= 0).getOrElse(true))
  assert((coreInstanceCount >= 1).getOrElse(true))

  def copy(id: PipelineObjectId = id,
    amiVersion: String = amiVersion,
    supportedProducts: Option[String] = supportedProducts,
    standardBootstrapAction: Seq[String] = standardBootstrapAction,
    bootstrapAction: Seq[String] = bootstrapAction,
    enableDebugging: Option[Boolean] = enableDebugging,
    hadoopSchedulerType: Option[SchedulerType] = hadoopSchedulerType,
    keyPair: Option[String] = keyPair,
    masterInstanceBidPrice: Option[HDouble] = masterInstanceBidPrice,
    masterInstanceType: Option[HString] = masterInstanceType,
    coreInstanceBidPrice: Option[HDouble] = coreInstanceBidPrice,
    coreInstanceCount: HInt = coreInstanceCount,
    coreInstanceType: Option[HString] = coreInstanceType,
    taskInstanceBidPrice: Option[HDouble] = taskInstanceBidPrice,
    taskInstanceCount: HInt = taskInstanceCount,
    taskInstanceType: Option[HString] = taskInstanceType,
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
    initTimeout: Option[HDuration] = initTimeout,
    terminateAfter: Option[HDuration] = terminateAfter,
    actionOnResourceFailure: Option[ActionOnResourceFailure] = actionOnResourceFailure,
    actionOnTaskFailure: Option[ActionOnTaskFailure] = actionOnTaskFailure
  ) = new MapReduceCluster(id, amiVersion, supportedProducts, standardBootstrapAction, bootstrapAction, enableDebugging,
    hadoopSchedulerType, keyPair, masterInstanceBidPrice, masterInstanceType,
    coreInstanceBidPrice, coreInstanceCount, coreInstanceType,
    taskInstanceBidPrice, taskInstanceCount, taskInstanceType, region, availabilityZone, resourceRole, role, subnetId,
    masterSecurityGroupId, additionalMasterSecurityGroupIds, slaveSecurityGroupId, additionalSlaveSecurityGroupIds,
    useOnDemandOnLastAttempt, visibleToAllUsers, initTimeout, terminateAfter,
    actionOnResourceFailure, actionOnTaskFailure)

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withAmiVersion(ver: String) = this.copy(amiVersion = ver)
  def withSupportedProducts(products: String) = this.copy(supportedProducts = Option(products))
  def withBootstrapAction(action: String*) = this.copy(bootstrapAction = bootstrapAction ++ action)
  def withDebuggingEnabled() = this.copy(enableDebugging = Option(true))
  def withHadoopSchedulerType(hadoopSchedulerType: SchedulerType) = this.copy(hadoopSchedulerType = Option(hadoopSchedulerType))
  def withKeyPair(keyPair: String) = this.copy(keyPair = Option(keyPair))
  def withMasterInstanceBidPrice(masterInstanceBidPrice: HDouble) = this.copy(masterInstanceBidPrice= Option(masterInstanceBidPrice))
  def withMasterInstanceType(instanceType: HString) = this.copy(masterInstanceType = Option(instanceType))
  def withCoreInstanceBidPrice(coreInstanceBidPrice: HDouble) = this.copy(coreInstanceBidPrice = Option(coreInstanceBidPrice))
  def withCoreInstanceCount(instanceCount: HInt) = this.copy(coreInstanceCount = instanceCount)
  def withCoreInstanceType(instanceType: HString) = this.copy(coreInstanceType = Option(instanceType))
  def withTaskInstanceBidPrice(bid: HDouble) = this.copy(taskInstanceBidPrice = Option(bid))
  def withTaskInstanceCount(instanceCount: HInt) = this.copy(taskInstanceCount = instanceCount)
  def withTaskInstanceType(instanceType: HString) = this.copy(taskInstanceType = Option(instanceType))
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
  def withInitTimeout(timeout: HDuration) = this.copy(initTimeout = Option(timeout))
  def terminatingAfter(terminateAfter: HDuration) = this.copy(terminateAfter = Option(terminateAfter))
  def withActionOnResourceFailure(actionOnResourceFailure: ActionOnResourceFailure) = this.copy(actionOnResourceFailure = Option(actionOnResourceFailure))
  def withActionOnTaskFailure(actionOnTaskFailure: ActionOnTaskFailure) = this.copy(actionOnTaskFailure = Option(actionOnTaskFailure))

  lazy val instanceCount: HInt = 1 + coreInstanceCount + taskInstanceCount

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
    masterInstanceType = masterInstanceType.map(_.toString),
    coreInstanceBidPrice = coreInstanceBidPrice.map(_.toString),
    coreInstanceCount = Option(coreInstanceCount.toString),
    coreInstanceType = coreInstanceType.map(_.toString),
    taskInstanceBidPrice = if (taskInstanceCount.isZero.getOrElse(false)) None else taskInstanceBidPrice.map(_.toString),
    taskInstanceCount = if (taskInstanceCount.isZero.getOrElse(false)) None else Option(taskInstanceCount.toString),
    taskInstanceType = if (taskInstanceCount.isZero.getOrElse(false)) None else taskInstanceType.map(_.toString),
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
    masterInstanceType = Option(hc.emrInstanceType: HString),
    coreInstanceBidPrice = None,
    coreInstanceCount = 2,
    coreInstanceType = Option(hc.emrInstanceType: HString),
    taskInstanceBidPrice = None,
    taskInstanceCount = 0,
    taskInstanceType = Option(hc.emrInstanceType: HString),
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
    terminateAfter = hc.emrTerminateAfter.map(duration2HDuration),
    actionOnResourceFailure = None,
    actionOnTaskFailure = None
  )
}
