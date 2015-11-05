package com.krux.hyperion.resource

import org.slf4j.LoggerFactory

import com.krux.hyperion.adt.HType._
import com.krux.hyperion.adt.{HInt, HDuration, HDouble, HString, HBoolean}
import com.krux.hyperion.aws.AdpEmrCluster
import com.krux.hyperion.common.{HttpProxy, PipelineObject, PipelineObjectId}
import com.krux.hyperion.HyperionContext

/**
 * Launch a Spark cluster
 */
class SparkCluster private (
  val id: PipelineObjectId,
  val sparkVersion: Option[HString],
  val amiVersion: Option[HString],
  val supportedProducts: Option[HString],
  val standardBootstrapAction: Seq[HString],
  val bootstrapAction: Seq[HString],
  val enableDebugging: Option[HBoolean],
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
  val useOnDemandOnLastAttempt: Option[HBoolean],
  val visibleToAllUsers: Option[HBoolean],
  val initTimeout: Option[HDuration],
  val terminateAfter: Option[HDuration],
  val actionOnResourceFailure: Option[ActionOnResourceFailure],
  val actionOnTaskFailure: Option[ActionOnTaskFailure],
  val httpProxy: Option[HttpProxy],
  val releaseLabel: Option[HString],
  val applications: Seq[HString],
  val configuration: Option[EmrConfiguration]
) extends EmrCluster {

  val logger = LoggerFactory.getLogger(SparkCluster.getClass)

  def copy(id: PipelineObjectId = id,
    sparkVersion: Option[HString] = sparkVersion,
    amiVersion: Option[HString] = amiVersion,
    supportedProducts: Option[HString] = supportedProducts,
    standardBootstrapAction: Seq[HString] = standardBootstrapAction,
    bootstrapAction: Seq[HString] = bootstrapAction,
    enableDebugging: Option[HBoolean] = enableDebugging,
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
    useOnDemandOnLastAttempt: Option[HBoolean] = useOnDemandOnLastAttempt,
    visibleToAllUsers: Option[HBoolean] = visibleToAllUsers,
    initTimeout: Option[HDuration] = initTimeout,
    terminateAfter: Option[HDuration] = terminateAfter,
    actionOnResourceFailure: Option[ActionOnResourceFailure] = actionOnResourceFailure,
    actionOnTaskFailure: Option[ActionOnTaskFailure] = actionOnTaskFailure,
    httpProxy: Option[HttpProxy] = httpProxy,
    releaseLabel: Option[HString] = releaseLabel,
    applications: Seq[HString] = applications,
    configuration: Option[EmrConfiguration] = configuration
  ) = new SparkCluster(id, sparkVersion, amiVersion, supportedProducts, standardBootstrapAction, bootstrapAction,
    enableDebugging, hadoopSchedulerType, keyPair, masterInstanceBidPrice, masterInstanceType,
    coreInstanceBidPrice, coreInstanceCount, coreInstanceType,
    taskInstanceBidPrice, taskInstanceCount, taskInstanceType, region, availabilityZone, resourceRole, role, subnetId,
    masterSecurityGroupId, additionalMasterSecurityGroupIds, slaveSecurityGroupId, additionalSlaveSecurityGroupIds,
    useOnDemandOnLastAttempt, visibleToAllUsers, initTimeout, terminateAfter,
    actionOnResourceFailure, actionOnTaskFailure, httpProxy, releaseLabel, applications, configuration)

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withSparkVersion(sparkVersion: HString) = this.copy(sparkVersion = Option(sparkVersion))
  def withAmiVersion(ver: HString) = this.copy(amiVersion = Option(ver))
  def withSupportedProducts(products: HString) = this.copy(supportedProducts = Option(products))
  def withBootstrapAction(action: HString*) = this.copy(bootstrapAction = bootstrapAction ++ action)
  def withDebuggingEnabled() = this.copy(enableDebugging = Option(HBoolean.True))
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
  def withUseOnDemandOnLastAttempt(useOnDemandOnLastAttempt: HBoolean) = this.copy(useOnDemandOnLastAttempt = Option(useOnDemandOnLastAttempt))
  def withVisibleToAllUsers(visibleToAllUsers: HBoolean) = this.copy(visibleToAllUsers = Option(visibleToAllUsers))
  def withInitTimeout(timeout: HDuration) = this.copy(initTimeout = Option(timeout))
  def terminatingAfter(terminateAfter: HDuration) = this.copy(terminateAfter = Option(terminateAfter))
  def withActionOnResourceFailure(actionOnResourceFailure: ActionOnResourceFailure) = this.copy(actionOnResourceFailure = Option(actionOnResourceFailure))
  def withActionOnTaskFailure(actionOnTaskFailure: ActionOnTaskFailure) = this.copy(actionOnTaskFailure = Option(actionOnTaskFailure))
  def withHttpProxy(proxy: HttpProxy) = this.copy(httpProxy = Option(proxy))
  def withReleaseLabel(releaseLabel: HString) = this.copy(releaseLabel = Option(releaseLabel), amiVersion = None)
  def withApplication(application: HString*) = this.copy(applications = this.applications ++ application)
  def withConfiguration(configuration: EmrConfiguration) = this.copy(configuration = Option(configuration))

  lazy val instanceCount = coreInstanceCount + taskInstanceCount + 1

  override def objects: Iterable[PipelineObject] = configuration.toList ++ httpProxy.toList

  lazy val serialize = {

    assert((coreInstanceCount >= 2).getOrElse {
      logger.warn("Server side expression cannot be evaluated. Unchecked comparison.")
      true
    })
    assert((taskInstanceCount >= 0).getOrElse {
      logger.warn(s"Server side expression: ${taskInstanceCount} cannot be evaluated. Unchecked comparison.")
      true
    })

    new AdpEmrCluster(
      id = id,
      name = id.toOption,
      amiVersion = amiVersion.map(_.serialize),
      supportedProducts = supportedProducts.map(_.serialize),
      bootstrapAction = Seq(s"s3://support.elasticmapreduce/spark/install-spark,-v,${sparkVersion.get},-x")
        ++ standardBootstrapAction.map(_.serialize) ++ bootstrapAction.map(_.serialize),
      enableDebugging = enableDebugging.map(_.serialize),
      hadoopSchedulerType = hadoopSchedulerType.map(_.serialize),
      keyPair = keyPair.map(_.serialize),
      masterInstanceBidPrice = masterInstanceBidPrice.map(_.serialize),
      masterInstanceType = masterInstanceType.map(_.serialize),
      coreInstanceBidPrice = coreInstanceBidPrice.map(_.serialize),
      coreInstanceCount = Option(coreInstanceCount.serialize),
      coreInstanceType = coreInstanceType.map(_.serialize),
      taskInstanceBidPrice = if (taskInstanceCount.isZero.getOrElse(false)) None else taskInstanceBidPrice.map(_.serialize),
      taskInstanceCount = if (taskInstanceCount.isZero.getOrElse(false)) None else Option(taskInstanceCount.serialize),
      taskInstanceType = if (taskInstanceCount.isZero.getOrElse(false)) None else taskInstanceType.map(_.serialize),
      region = region.map(_.serialize),
      availabilityZone = availabilityZone.map(_.serialize),
      resourceRole = resourceRole.map(_.serialize),
      role = role.map(_.serialize),
      subnetId = subnetId.map(_.serialize),
      masterSecurityGroupId = masterSecurityGroupId.map(_.serialize),
      additionalMasterSecurityGroupIds = additionalMasterSecurityGroupIds match {
        case Seq() => None
        case groupIds => Option(groupIds.map(_.serialize))
      },
      slaveSecurityGroupId = slaveSecurityGroupId.map(_.serialize),
      additionalSlaveSecurityGroupIds = additionalSlaveSecurityGroupIds match {
        case Seq() => None
        case groupIds => Option(groupIds.map(_.serialize))
      },
      useOnDemandOnLastAttempt = useOnDemandOnLastAttempt.map(_.serialize),
      visibleToAllUsers = visibleToAllUsers.map(_.serialize),
      initTimeout = initTimeout.map(_.serialize),
      terminateAfter = terminateAfter.map(_.serialize),
      actionOnResourceFailure = actionOnResourceFailure.map(_.serialize),
      actionOnTaskFailure = actionOnTaskFailure.map(_.serialize),
      httpProxy = httpProxy.map(_.ref),
      releaseLabel = releaseLabel.map(_.serialize),
      applications = applications.map(_.serialize),
      configuration = configuration.map(_.ref)
    )
  }

}

object SparkCluster {

  def apply()(implicit hc: HyperionContext): SparkCluster = apply(None)

  def apply(configuration: EmrConfiguration)(implicit hc: HyperionContext): SparkCluster = apply(Option(configuration))

  private def apply(configuration: Option[EmrConfiguration])(implicit hc: HyperionContext): SparkCluster = new SparkCluster(
    id = PipelineObjectId(SparkCluster.getClass),
    sparkVersion = hc.emrSparkVersion,
    amiVersion = hc.emrAmiVersion,
    supportedProducts = None,
    standardBootstrapAction = hc.emrEnvironmentUri
      .map(env => s"${hc.scriptUri}deploy-hyperion-emr-env.sh,$env": HString).toList,
    bootstrapAction = Seq.empty,
    enableDebugging = None,
    hadoopSchedulerType = None,
    keyPair = hc.emrKeyPair.map(_.serialize),
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
    terminateAfter = hc.emrTerminateAfter.map(x => x: HDuration),
    actionOnResourceFailure = None,
    actionOnTaskFailure = None,
    httpProxy = None,
    releaseLabel = hc.emrReleaseLabel,
    applications = Seq.empty,
    configuration = configuration
  )

}
