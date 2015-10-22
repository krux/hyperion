package com.krux.hyperion.resource

import com.krux.hyperion.aws.AdpEmrCluster
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.adt.HType._
import com.krux.hyperion.adt.{HInt, HDuration, HDouble, HString}
import com.krux.hyperion.HyperionContext

/**
 * Launch a Spark cluster
 */
class SparkCluster private (
  val id: PipelineObjectId,
  val sparkVersion: HString,
  val amiVersion: HString,
  val supportedProducts: Option[HString],
  val standardBootstrapAction: Seq[HString],
  val bootstrapAction: Seq[HString],
  val enableDebugging: Option[Boolean],
  val hadoopSchedulerType: Option[SchedulerType],
  val keyPair: Option[HString],
  val masterInstanceBidPrice: Option[HDouble],
  val masterInstanceType: Option[HString],
  val coreInstanceBidPrice: Option[HDouble],
  val coreInstanceCount: HInt,
  val coreInstanceType: Option[HString],
  val taskInstanceBidPrice: Option[HDouble],
  val taskInstanceCount: HInt,
  val taskInstanceType: Option[HString],
  val region: Option[HString],
  val availabilityZone: Option[HString],
  val resourceRole: Option[HString],
  val role: Option[HString],
  val subnetId: Option[HString],
  val masterSecurityGroupId: Option[HString],
  val additionalMasterSecurityGroupIds: Seq[HString],
  val slaveSecurityGroupId: Option[HString],
  val additionalSlaveSecurityGroupIds: Seq[HString],
  val useOnDemandOnLastAttempt: Option[Boolean],
  val visibleToAllUsers: Option[Boolean],
  val initTimeout: Option[HDuration],
  val terminateAfter: Option[HDuration],
  val actionOnResourceFailure: Option[ActionOnResourceFailure],
  val actionOnTaskFailure: Option[ActionOnTaskFailure]
) extends EmrCluster {

  assert((coreInstanceCount >= 2).getOrElse(true))
  assert((taskInstanceCount >= 0).getOrElse(true))

  def copy(id: PipelineObjectId = id,
    sparkVersion: HString = sparkVersion,
    amiVersion: HString = amiVersion,
    supportedProducts: Option[HString] = supportedProducts,
    standardBootstrapAction: Seq[HString] = standardBootstrapAction,
    bootstrapAction: Seq[HString] = bootstrapAction,
    enableDebugging: Option[Boolean] = enableDebugging,
    hadoopSchedulerType: Option[SchedulerType] = hadoopSchedulerType,
    keyPair: Option[HString] = keyPair,
    masterInstanceBidPrice: Option[HDouble] = masterInstanceBidPrice,
    masterInstanceType: Option[HString] = masterInstanceType,
    coreInstanceBidPrice: Option[HDouble] = coreInstanceBidPrice,
    coreInstanceCount: HInt = coreInstanceCount,
    coreInstanceType: Option[HString] = coreInstanceType,
    taskInstanceBidPrice: Option[HDouble] = taskInstanceBidPrice,
    taskInstanceCount: HInt = taskInstanceCount,
    taskInstanceType: Option[HString] = taskInstanceType,
    region: Option[HString] = region,
    availabilityZone: Option[HString] = availabilityZone,
    resourceRole: Option[HString] = resourceRole,
    role: Option[HString] = role,
    subnetId: Option[HString] = subnetId,
    masterSecurityGroupId: Option[HString] = masterSecurityGroupId,
    additionalMasterSecurityGroupIds: Seq[HString] = additionalMasterSecurityGroupIds,
    slaveSecurityGroupId: Option[HString] = slaveSecurityGroupId,
    additionalSlaveSecurityGroupIds: Seq[HString] = additionalSlaveSecurityGroupIds,
    useOnDemandOnLastAttempt: Option[Boolean] = useOnDemandOnLastAttempt,
    visibleToAllUsers: Option[Boolean] = visibleToAllUsers,
    initTimeout: Option[HDuration] = initTimeout,
    terminateAfter: Option[HDuration] = terminateAfter,
    actionOnResourceFailure: Option[ActionOnResourceFailure] = actionOnResourceFailure,
    actionOnTaskFailure: Option[ActionOnTaskFailure] = actionOnTaskFailure
  ) = new SparkCluster(id, sparkVersion, amiVersion, supportedProducts, standardBootstrapAction, bootstrapAction,
    enableDebugging, hadoopSchedulerType, keyPair, masterInstanceBidPrice, masterInstanceType,
    coreInstanceBidPrice, coreInstanceCount, coreInstanceType,
    taskInstanceBidPrice, taskInstanceCount, taskInstanceType, region, availabilityZone, resourceRole, role, subnetId,
    masterSecurityGroupId, additionalMasterSecurityGroupIds, slaveSecurityGroupId, additionalSlaveSecurityGroupIds,
    useOnDemandOnLastAttempt, visibleToAllUsers, initTimeout, terminateAfter,
    actionOnResourceFailure, actionOnTaskFailure)

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withSparkVersion(sparkVersion: HString) = this.copy(sparkVersion = sparkVersion)
  def withAmiVersion(ver: HString) = this.copy(amiVersion = ver)
  def withSupportedProducts(products: HString) = this.copy(supportedProducts = Option(products))
  def withBootstrapAction(action: HString*) = this.copy(bootstrapAction = bootstrapAction ++ action)
  def withDebuggingEnabled() = this.copy(enableDebugging = Option(true))
  def withHadoopSchedulerType(hadoopSchedulerType: SchedulerType) = this.copy(hadoopSchedulerType = Option(hadoopSchedulerType))
  def withKeyPair(keyPair: HString) = this.copy(keyPair = Option(keyPair))
  def withMasterInstanceBidPrice(masterInstanceBidPrice: HDouble) = this.copy(masterInstanceBidPrice= Option(masterInstanceBidPrice))
  def withMasterInstanceType(instanceType: HString) = this.copy(masterInstanceType = Option(instanceType))
  def withCoreInstanceBidPrice(coreInstanceBidPrice: HDouble) = this.copy(coreInstanceBidPrice = Option(coreInstanceBidPrice))
  def withCoreInstanceCount(instanceCount: HInt) = this.copy(coreInstanceCount = instanceCount)
  def withCoreInstanceType(instanceType: HString) = this.copy(coreInstanceType = Option(instanceType))
  def withTaskInstanceBidPrice(bid: HDouble) = this.copy(taskInstanceBidPrice = Option(bid))
  def withTaskInstanceCount(instanceCount: HInt) = this.copy(taskInstanceCount = instanceCount)
  def withTaskInstanceType(instanceType: HString) = this.copy(taskInstanceType = Option(instanceType))
  def withRegion(region: HString) = this.copy(region = Option(region))
  def withAvailabilityZone(availabilityZone: HString) = this.copy(availabilityZone = Option(availabilityZone))
  def withResourceRole(role: HString) = this.copy(resourceRole = Option(role))
  def withRole(role: HString) = this.copy(role = Option(role))
  def withSubnetId(id: HString) = this.copy(subnetId = Option(id))
  def withMasterSecurityGroupId(masterSecurityGroupId: HString) = this.copy(masterSecurityGroupId = Option(masterSecurityGroupId))
  def withAdditionalMasterSecurityGroupIds(securityGroupId: HString*) = this.copy(additionalMasterSecurityGroupIds = additionalMasterSecurityGroupIds ++ securityGroupId)
  def withSlaveSecurityGroupId(slaveSecurityGroupId: HString) = this.copy(slaveSecurityGroupId = Option(slaveSecurityGroupId))
  def withAdditionalSlaveSecurityGroupIds(securityGroupIds: HString*) = this.copy(additionalSlaveSecurityGroupIds = additionalSlaveSecurityGroupIds ++ securityGroupIds)
  def withUseOnDemandOnLastAttempt(useOnDemandOnLastAttempt: Boolean) = this.copy(useOnDemandOnLastAttempt = Option(useOnDemandOnLastAttempt))
  def withVisibleToAllUsers(visibleToAllUsers: Boolean) = this.copy(visibleToAllUsers = Option(visibleToAllUsers))
  def withInitTimeout(timeout: HDuration) = this.copy(initTimeout = Option(timeout))
  def terminatingAfter(terminateAfter: HDuration) = this.copy(terminateAfter = Option(terminateAfter))
  def withActionOnResourceFailure(actionOnResourceFailure: ActionOnResourceFailure) = this.copy(actionOnResourceFailure = Option(actionOnResourceFailure))
  def withActionOnTaskFailure(actionOnTaskFailure: ActionOnTaskFailure) = this.copy(actionOnTaskFailure = Option(actionOnTaskFailure))

  lazy val instanceCount = coreInstanceCount + taskInstanceCount + 1

  lazy val serialize = new AdpEmrCluster(
    id = id,
    name = id.toOption,
    amiVersion = Option(amiVersion.toAws),
    supportedProducts = supportedProducts.map(_.toAws),
    bootstrapAction = Seq(s"s3://support.elasticmapreduce/spark/install-spark,-v,$sparkVersion,-x")
      ++ standardBootstrapAction.map(_.toAws) ++ bootstrapAction.map(_.toAws),
    enableDebugging = enableDebugging.map(_.toString),
    hadoopSchedulerType = hadoopSchedulerType.map(_.toString),
    keyPair = keyPair.map(_.toAws),
    masterInstanceBidPrice = masterInstanceBidPrice.map(_.toAws),
    masterInstanceType = masterInstanceType.map(_.toAws),
    coreInstanceBidPrice = coreInstanceBidPrice.map(_.toAws),
    coreInstanceCount = Option(coreInstanceCount.toAws),
    coreInstanceType = coreInstanceType.map(_.toAws),
    taskInstanceBidPrice = if (taskInstanceCount.isZero.getOrElse(false)) None else taskInstanceBidPrice.map(_.toAws),
    taskInstanceCount = if (taskInstanceCount.isZero.getOrElse(false)) None else Option(taskInstanceCount.toAws),
    taskInstanceType = if (taskInstanceCount.isZero.getOrElse(false)) None else taskInstanceType.map(_.toAws),
    region = region.map(_.toAws),
    availabilityZone = availabilityZone.map(_.toAws),
    resourceRole = resourceRole.map(_.toAws),
    role = role.map(_.toAws),
    subnetId = subnetId.map(_.toAws),
    masterSecurityGroupId = masterSecurityGroupId.map(_.toAws),
    additionalMasterSecurityGroupIds = additionalMasterSecurityGroupIds match {
      case Seq() => None
      case groupIds => Option(groupIds.map(_.toAws))
    },
    slaveSecurityGroupId = slaveSecurityGroupId.map(_.toAws),
    additionalSlaveSecurityGroupIds = additionalSlaveSecurityGroupIds match {
      case Seq() => None
      case groupIds => Option(groupIds.map(_.toAws))
    },
    useOnDemandOnLastAttempt = useOnDemandOnLastAttempt.map(_.toString),
    visibleToAllUsers = visibleToAllUsers.map(_.toString),
    initTimeout = initTimeout.map(_.toString),
    terminateAfter = terminateAfter.map(_.toString),
    actionOnResourceFailure = actionOnResourceFailure.map(_.toString),
    actionOnTaskFailure = actionOnTaskFailure.map(_.toString)
  )

}

object SparkCluster {

  def apply()(implicit hc: HyperionContext) = new SparkCluster(
    id = PipelineObjectId(SparkCluster.getClass),
    sparkVersion = hc.emrSparkVersion,
    amiVersion = hc.emrAmiVersion,
    supportedProducts = None,
    standardBootstrapAction = hc.emrEnvironmentUri
      .map(env => s"${hc.scriptUri}deploy-hyperion-emr-env.sh,$env": HString).toList,
    bootstrapAction = Seq.empty,
    enableDebugging = None,
    hadoopSchedulerType = None,
    keyPair = hc.emrKeyPair.map(_.toAws),
    masterInstanceBidPrice = None,
    masterInstanceType = Option(hc.emrInstanceType: HString),
    coreInstanceBidPrice = None,
    coreInstanceCount = 2,
    coreInstanceType = Option(hc.emrInstanceType: HString),
    taskInstanceBidPrice = None,
    taskInstanceCount = 0,
    taskInstanceType = Option(hc.emrInstanceType: HString),
    region = Option(hc.emrRegion: HString),
    availabilityZone = hc.emrAvailabilityZone.map(a => a: HString),
    resourceRole = Option(hc.emrResourceRole: HString),
    role = Option(hc.emrRole: HString),
    subnetId = hc.emrSubnetId.map(a => a: HString),
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
