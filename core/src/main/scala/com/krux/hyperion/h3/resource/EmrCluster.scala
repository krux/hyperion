package com.krux.hyperion.h3.resource

import shapeless._
import org.slf4j.Logger

import com.krux.hyperion.adt.HType._
import com.krux.hyperion.adt.{ HInt, HDouble, HString, HBoolean }
import com.krux.hyperion.aws.{ AdpRef, AdpEmrCluster }


trait EmrCluster extends ResourceObject {

  type Self <: EmrCluster

  def logger: Logger

  def emrClusterFieldsLens: Lens[Self, EmrClusterFields]

  private val amiVersionLens = emrClusterFieldsLens >> 'amiVersion
  def amiVersion = amiVersionLens.get(self)
  def withAmiVersion(version: HString): Self = amiVersionLens.set(self)(Option(version))

  private val supportedProductsLens = emrClusterFieldsLens >> 'supportedProducts
  def supportedProducts = supportedProductsLens.get(self)
  def withSupportedProducts(products: HString): Self = supportedProductsLens.set(self)(Option(products))

  private val standardBootstrapActionLens = emrClusterFieldsLens >> 'standardBootstrapAction
  def standardBootstrapAction = standardBootstrapActionLens.get(self)
  def withStandardBootstrapAction(actions: HString*): Self = standardBootstrapActionLens.modify(self)(_ ++ actions)

  private val bootstrapActionLens = emrClusterFieldsLens >> 'bootstrapAction
  def bootstrapAction = bootstrapActionLens.get(self)
  def withBootstrapAction(actions: HString*): Self = bootstrapActionLens.modify(self)(_ ++ actions)

  private val enableDebuggingLens = emrClusterFieldsLens >> 'enableDebugging
  def enableDebugging = enableDebuggingLens.get(self)
  def withDebuggingEnabled(enabled: HBoolean): Self = enableDebuggingLens.set(self)(Option(enabled))

  private val hadoopSchedulerTypeLens = emrClusterFieldsLens >> 'hadoopSchedulerType
  def hadoopSchedulerType = hadoopSchedulerTypeLens.get(self)
  def withHadoopSchedulerType(scheduleType: SchedulerType): Self = hadoopSchedulerTypeLens.set(self)(Option(scheduleType))

  private val masterInstanceBidPriceLens = emrClusterFieldsLens >> 'masterInstanceBidPrice
  def masterInstanceBidPrice = masterInstanceBidPriceLens.get(self)
  def withMasterInstanceBidPrice(price: HDouble): Self = masterInstanceBidPriceLens.set(self)(Option(price))

  private val masterInstanceTypeLens = emrClusterFieldsLens >> 'masterInstanceType
  def masterInstanceType = masterInstanceTypeLens.get(self)
  def withMasterInstanceType(instanceType: HString): Self = masterInstanceTypeLens.set(self)(Option(instanceType))

  private val coreInstanceBidPriceLens = emrClusterFieldsLens >> 'coreInstanceBidPrice
  def coreInstanceBidPrice = coreInstanceBidPriceLens.get(self)
  def withCoreInstanceBidPrice(price: HDouble): Self = coreInstanceBidPriceLens.set(self)(Option(price))

  private val coreInstanceCountLens = emrClusterFieldsLens >> 'coreInstanceCount
  def coreInstanceCount = coreInstanceCountLens.get(self)
  def withCoreInstanceCount(count: HInt): Self = coreInstanceCountLens.set(self)(count)

  private val coreInstanceTypeLens = emrClusterFieldsLens >> 'coreInstanceType
  def coreInstanceType = coreInstanceTypeLens.get(self)
  def withCoreInstanceType(instanceType: HString): Self = coreInstanceTypeLens.set(self)(Option(instanceType))

  private val taskInstanceBidPriceLens = emrClusterFieldsLens >> 'taskInstanceBidPrice
  def taskInstanceBidPrice = taskInstanceBidPriceLens.get(self)
  def withTaskInstanceBidPrice(price: HDouble): Self = taskInstanceBidPriceLens.set(self)(Option(price))

  private val taskInstanceCountLens = emrClusterFieldsLens >> 'taskInstanceCount
  def taskInstanceCount = taskInstanceCountLens.get(self)
  def withTaskInstanceCount(count: HInt): Self = taskInstanceCountLens.set(self)(count)

  private val taskInstanceTypeLens = emrClusterFieldsLens >> 'taskInstanceType
  def taskInstanceType = taskInstanceTypeLens.get(self)
  def withTaskInstanceType(instanceType: HString): Self = taskInstanceTypeLens.set(self)(Option(instanceType))

  private val masterSecurityGroupIdLens = emrClusterFieldsLens >> 'masterSecurityGroupId
  def masterSecurityGroupId = masterSecurityGroupIdLens.get(self)
  def withMasterSecurityGroupId(groupId: HString): Self = masterSecurityGroupIdLens.set(self)(Option(groupId))

  private val additionalMasterSecurityGroupIdsLens = emrClusterFieldsLens >> 'additionalMasterSecurityGroupIds
  def additionalMasterSecurityGroupIds = additionalMasterSecurityGroupIdsLens.get(self)
  def withAdditionalMasterSecurityGroupIds(groupIds: HString*): Self = additionalMasterSecurityGroupIdsLens.modify(self)(_ ++ groupIds)

  private val slaveSecurityGroupIdLens = emrClusterFieldsLens >> 'slaveSecurityGroupId
  def slaveSecurityGroupId = slaveSecurityGroupIdLens.get(self)
  def withSlaveSecurityGroupId(groupId: HString): Self = slaveSecurityGroupIdLens.set(self)(Option(groupId))

  private val additionalSlaveSecurityGroupIdsLens = emrClusterFieldsLens >> 'additionalSlaveSecurityGroupIds
  def additionalSlaveSecurityGroupIds = additionalSlaveSecurityGroupIdsLens.get(self)
  def withAdditionalSlaveSecurityGroupIds(groupIds: HString*): Self = additionalSlaveSecurityGroupIdsLens.modify(self)(_ ++ groupIds)

  private val visibleToAllUsersLens = emrClusterFieldsLens >> 'visibleToAllUsers
  def visibleToAllUsers = visibleToAllUsersLens.get(self)
  def withVisibleToAllUsers(visible: HBoolean): Self = visibleToAllUsersLens.set(self)(Option(visible))

  private val releaseLabelLens = emrClusterFieldsLens >> 'releaseLabel
  def releaseLabel = releaseLabelLens.get(self)
  def withReleaseLabel(label: HString): Self = releaseLabelLens.set(self)(Option(label))

  private val applicationsLens = emrClusterFieldsLens >> 'applications
  def applications = applicationsLens.get(self)
  def withApplications(apps: HString*): Self = applicationsLens.modify(self)(_ ++ apps)

  private val configurationLens = emrClusterFieldsLens >> 'configuration
  def configuration = configurationLens.get(self)
  def withConfiguration(conf: EmrConfiguration): Self = configurationLens.set(self)(Option(conf))

  override def ref: AdpRef[AdpEmrCluster] = AdpRef(serialize)

  lazy val instanceCount: HInt = 1 + coreInstanceCount + taskInstanceCount

  lazy val serialize = {

    assert((taskInstanceCount >= 0).getOrElse {
      logger.warn("Server side expression cannot be evaluated. Unchecked comparison.")
      true
    })

    assert((coreInstanceCount >= 1).getOrElse {
      logger.warn("Server side expression cannot be evaluated. Unchecked comparison.")
      true
    })

    new AdpEmrCluster(
      id = id,
      name = id.toOption,
      amiVersion = amiVersion.map(_.serialize),
      supportedProducts = supportedProducts.map(_.serialize),
      bootstrapAction = (standardBootstrapAction ++ bootstrapAction).map(_.serialize),
      enableDebugging = enableDebugging.map(_.serialize),
      hadoopSchedulerType = hadoopSchedulerType.map(_.serialize),
      keyPair = keyPair.map(_.serialize),
      masterInstanceBidPrice = masterInstanceBidPrice.map(_.serialize),
      masterInstanceType = masterInstanceType.map(_.serialize),
      coreInstanceBidPrice = coreInstanceBidPrice.map(_.serialize),
      coreInstanceCount = Option(coreInstanceCount.serialize),
      coreInstanceType = coreInstanceType.map(_.serialize),
      taskInstanceBidPrice = if (taskInstanceCount.isZero.forall(! _)) taskInstanceBidPrice.map(_.serialize) else None,
      taskInstanceCount = if (taskInstanceCount.isZero.forall(! _)) Option(taskInstanceCount.serialize) else None,
      taskInstanceType = if (taskInstanceCount.isZero.forall(! _)) taskInstanceType.map(_.serialize) else None,
      region = region.map(_.serialize),
      availabilityZone = availabilityZone.map(_.serialize),
      resourceRole = resourceRole.map(_.serialize),
      role = role.map(_.serialize),
      subnetId = subnetId.map(_.serialize),
      masterSecurityGroupId = masterSecurityGroupId.map(_.serialize),
      additionalMasterSecurityGroupIds = additionalMasterSecurityGroupIds.map(_.serialize),
      slaveSecurityGroupId = slaveSecurityGroupId.map(_.serialize),
      additionalSlaveSecurityGroupIds = additionalSlaveSecurityGroupIds.map(_.serialize),
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
