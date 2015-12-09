package com.krux.hyperion.h3.resource

import shapeless._

import com.krux.hyperion.adt.HType._
import com.krux.hyperion.adt.{ HInt, HDouble, HString, HBoolean }
import com.krux.hyperion.aws.{ AdpRef, AdpEmrCluster }


trait EmrCluster extends ResourceObject {

  type Self <: EmrCluster

  def emrFieldsLens: Lens[Self, EmrClusterFields]

  private val amiVersionLens = emrFieldsLens >> 'amiVersion
  def amiVersion = amiVersionLens.get(self)
  def withAmiVersion(version: HString) = amiVersionLens.set(self)(Option(version))

  private val supportedProductsLens = emrFieldsLens >> 'supportedProducts
  def supportedProducts = supportedProductsLens.get(self)
  def withSupportedProducts(products: HString) = supportedProductsLens.set(self)(Option(products))

  private val standardBootstrapActionLens = emrFieldsLens >> 'standardBootstrapAction
  def standardBootstrapAction = standardBootstrapActionLens.get(self)
  def withStandardBootstrapAction(actions: HString*) = standardBootstrapActionLens.modify(self)(_ ++ actions)

  private val bootstrapActionLens = emrFieldsLens >> 'bootstrapAction
  def bootstrapAction = bootstrapActionLens.get(self)
  def withBootstrapAction(actions: HString*) = bootstrapActionLens.modify(self)(_ ++ actions)

  private val enableDebuggingLens = emrFieldsLens >> 'enableDebugging
  def enableDebugging = enableDebuggingLens.get(self)
  def withDebuggingEnabled(enabled: HBoolean) = enableDebuggingLens.set(self)(Option(enabled))

  private val hadoopSchedulerTypeLens = emrFieldsLens >> 'hadoopSchedulerType
  def hadoopSchedulerType = hadoopSchedulerTypeLens.get(self)
  def withHadoopSchedulerType(scheduleType: SchedulerType) = hadoopSchedulerTypeLens.set(self)(Option(scheduleType))

  private val coreInstanceBidPriceLens = emrFieldsLens >> 'coreInstanceBidPrice
  def coreInstanceBidPrice = coreInstanceBidPriceLens.get(self)
  def withCoreInstanceBidPrice(price: HDouble) = coreInstanceBidPriceLens.set(self)(Option(price))

  private val coreInstanceCountLens = emrFieldsLens >> 'coreInstanceCount
  def coreInstanceCount = coreInstanceCountLens.get(self)
  def withCoreInstanceCount(count: HInt) = coreInstanceCountLens.set(self)(count)

  private val coreInstanceTypeLens = emrFieldsLens >> 'coreInstanceType
  def coreInstanceType = coreInstanceTypeLens.get(self)
  def withCoreInstanceType(instanceType: HString) = coreInstanceTypeLens.set(self)(Option(instanceType))

  private val taskInstanceBidPriceLens = emrFieldsLens >> 'taskInstanceBidPrice
  def taskInstanceBidPrice = taskInstanceBidPriceLens.get(self)
  def withTaskInstanceBidPrice(price: HDouble) = taskInstanceBidPriceLens.set(self)(Option(price))

  private val taskInstanceCountLens = emrFieldsLens >> 'taskInstanceCount
  def taskInstanceCount = taskInstanceCountLens.get(self)
  def withTaskInstanceCount(count: HInt) = taskInstanceCountLens.set(self)(count)

  private val taskInstanceTypeLens = emrFieldsLens >> 'taskInstanceType
  def taskInstanceType = taskInstanceTypeLens.get(self)
  def withTaskInstanceType(instanceType: HString) = taskInstanceTypeLens.set(self)(Option(instanceType))

  private val masterSecurityGroupIdLens = emrFieldsLens >> 'masterSecurityGroupId
  def masterSecurityGroupId = masterSecurityGroupIdLens.get(self)
  def withMasterSecurityGroupId(groupId: HString) = masterSecurityGroupIdLens.set(self)(Option(groupId))

  private val additionalMasterSecurityGroupIdsLens = emrFieldsLens >> 'additionalMasterSecurityGroupIds
  def additionalMasterSecurityGroupIds = additionalMasterSecurityGroupIdsLens.get(self)
  def withAdditionalMasterSecurityGroupIds(groupIds: HString*) = additionalMasterSecurityGroupIdsLens.modify(self)(_ ++ groupIds)

  private val slaveSecurityGroupIdLens = emrFieldsLens >> 'slaveSecurityGroupId
  def slaveSecurityGroupId = slaveSecurityGroupIdLens.get(self)
  def withSlaveSecurityGroupId(groupId: HString) = slaveSecurityGroupIdLens.set(self)(Option(groupId))

  private val additionalSlaveSecurityGroupIdsLens = emrFieldsLens >> 'additionalSlaveSecurityGroupIds
  def additionalSlaveSecurityGroupIds = additionalSlaveSecurityGroupIdsLens.get(self)
  def withAdditionalSlaveSecurityGroupIds(groupIds: HString*) = additionalSlaveSecurityGroupIdsLens.modify(self)(_ ++ groupIds)

  private val visibleToAllUsersLens = emrFieldsLens >> 'visibleToAllUsers
  def visibleToAllUsers = visibleToAllUsersLens.get(self)
  def withVisibleToAllUsers(visible: HBoolean) = visibleToAllUsersLens.set(self)(Option(visible))

  private val releaseLabelLens = emrFieldsLens >> 'releaseLabel
  def releaseLabel = releaseLabelLens.get(self)
  def withReleaseLabel(label: HString) = releaseLabelLens.set(self)(Option(label))

  private val applicationsLens = emrFieldsLens >> 'applications
  def applications = applicationsLens.get(self)
  def withApplications(apps: HString*) = applicationsLens.modify(self)(_ ++ apps)

  private val configurationLens = emrFieldsLens >> 'configuration
  def configuration = configurationLens.get(self)
  def withConfiguration(conf: EmrConfiguration) = configurationLens.set(self)(Option(conf))

  def serialize: AdpEmrCluster

  def ref: AdpRef[AdpEmrCluster] = AdpRef(serialize)

}
