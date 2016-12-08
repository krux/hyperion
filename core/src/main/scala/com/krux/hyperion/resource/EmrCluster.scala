package com.krux.hyperion.resource

import org.slf4j.Logger

import com.krux.hyperion.adt.{ HInt, HDouble, HString, HBoolean, HDuration }
import com.krux.hyperion.aws.{ AdpRef, AdpEmrCluster }
import com.krux.hyperion.HyperionContext

trait EmrCluster extends ResourceObject {

  type Self <: EmrCluster

  def logger: Logger

  def emrClusterFields: EmrClusterFields
  def updateEmrClusterFields(fields: EmrClusterFields): Self

  def amiVersion = emrClusterFields.amiVersion
  def withAmiVersion(version: HString): Self = updateEmrClusterFields(
    emrClusterFields.copy(amiVersion = Option(version), releaseLabel = None)
  )

  def supportedProducts = emrClusterFields.supportedProducts
  def withSupportedProducts(products: HString): Self = updateEmrClusterFields(
    emrClusterFields.copy(supportedProducts = Option(products))
  )

  def standardBootstrapAction: Seq[HString] = if (releaseLabel.nonEmpty)
    Seq.empty[HString]
  else
    emrClusterFields.standardBootstrapAction

  def withStandardBootstrapAction(actions: HString*): Self = updateEmrClusterFields(
    emrClusterFields.copy(standardBootstrapAction = emrClusterFields.standardBootstrapAction ++ actions)
  )

  def bootstrapAction = emrClusterFields.bootstrapAction
  def withBootstrapAction(actions: HString*): Self = updateEmrClusterFields(
    emrClusterFields.copy(bootstrapAction = emrClusterFields.bootstrapAction ++ actions)
  )

  def enableDebugging = emrClusterFields.enableDebugging
  def withDebuggingEnabled(enabled: HBoolean): Self = updateEmrClusterFields(
    emrClusterFields.copy(enableDebugging = Option(enabled))
  )

  def hadoopSchedulerType = emrClusterFields.hadoopSchedulerType
  def withHadoopSchedulerType(scheduleType: SchedulerType): Self = updateEmrClusterFields(
    emrClusterFields.copy(hadoopSchedulerType = Option(scheduleType))
  )

  def masterInstanceBidPrice = emrClusterFields.masterInstanceBidPrice
  def withMasterInstanceBidPrice(price: HDouble): Self = updateEmrClusterFields(
    emrClusterFields.copy(masterInstanceBidPrice = Option(price))
  )

  def masterInstanceType = emrClusterFields.masterInstanceType
  def withMasterInstanceType(instanceType: HString): Self = updateEmrClusterFields(
    emrClusterFields.copy(masterInstanceType = Option(instanceType))
  )

  def coreInstanceBidPrice = emrClusterFields.coreInstanceBidPrice
  def withCoreInstanceBidPrice(price: HDouble): Self = updateEmrClusterFields(
    emrClusterFields.copy(coreInstanceBidPrice = Option(price))
  )

  def coreInstanceCount = emrClusterFields.coreInstanceCount
  def withCoreInstanceCount(count: HInt): Self = updateEmrClusterFields(
    emrClusterFields.copy(coreInstanceCount = count)
  )

  def coreInstanceType = emrClusterFields.coreInstanceType
  def withCoreInstanceType(instanceType: HString): Self = updateEmrClusterFields(
    emrClusterFields.copy(coreInstanceType = Option(instanceType))
  )

  def taskInstanceBidPrice = emrClusterFields.taskInstanceBidPrice
  def withTaskInstanceBidPrice(price: HDouble): Self = updateEmrClusterFields(
    emrClusterFields.copy(taskInstanceBidPrice = Option(price))
  )

  def taskInstanceCount = emrClusterFields.taskInstanceCount
  def withTaskInstanceCount(count: HInt): Self = updateEmrClusterFields(
    emrClusterFields.copy(taskInstanceCount = count)
  )

  def taskInstanceType = emrClusterFields.taskInstanceType
  def withTaskInstanceType(instanceType: HString): Self = updateEmrClusterFields(
    emrClusterFields.copy(taskInstanceType = Option(instanceType))
  )

  def masterSecurityGroupId = emrClusterFields.masterSecurityGroupId
  def withMasterSecurityGroupId(groupId: HString): Self = updateEmrClusterFields(
    emrClusterFields.copy(masterSecurityGroupId = Option(groupId))
  )

  def additionalMasterSecurityGroupIds = emrClusterFields.additionalMasterSecurityGroupIds
  def withAdditionalMasterSecurityGroupIds(groupIds: HString*): Self =  updateEmrClusterFields(
    emrClusterFields.copy(additionalMasterSecurityGroupIds = emrClusterFields.additionalMasterSecurityGroupIds ++ groupIds)
  )

  def slaveSecurityGroupId = emrClusterFields.slaveSecurityGroupId
  def withSlaveSecurityGroupId(groupId: HString): Self = updateEmrClusterFields(
    emrClusterFields.copy(slaveSecurityGroupId = Option(groupId))
  )

  def additionalSlaveSecurityGroupIds = emrClusterFields.additionalSlaveSecurityGroupIds
  def withAdditionalSlaveSecurityGroupIds(groupIds: HString*): Self =  updateEmrClusterFields(
    emrClusterFields.copy(additionalSlaveSecurityGroupIds = emrClusterFields.additionalSlaveSecurityGroupIds ++ groupIds)
  )

  def visibleToAllUsers = emrClusterFields.visibleToAllUsers
  def withVisibleToAllUsers(visible: HBoolean): Self = updateEmrClusterFields(
    emrClusterFields.copy(visibleToAllUsers = Option(visible))
  )

  def releaseLabel = emrClusterFields.releaseLabel
  def withReleaseLabel(label: HString): Self = updateEmrClusterFields(
    emrClusterFields.copy(releaseLabel = Option(label), amiVersion = None)
  )

  def applications = emrClusterFields.applications
  def withApplications(apps: HString*): Self = updateEmrClusterFields(
    emrClusterFields.copy(applications = emrClusterFields.applications ++ apps)
  )

  def configuration = emrClusterFields.configuration
  def withConfiguration(conf: EmrConfiguration*): Self = updateEmrClusterFields(
    emrClusterFields.copy(configuration = emrClusterFields.configuration ++ conf)
  )

  override def objects = configuration ++ super.objects

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
      name = name,
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
      emrManagedMasterSecurityGroupId = masterSecurityGroupId.map(_.serialize),
      additionalMasterSecurityGroupIds = additionalMasterSecurityGroupIds.map(_.serialize),
      emrManagedSlaveSecurityGroupId = slaveSecurityGroupId.map(_.serialize),
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
      configuration = configuration.map(_.ref),
      maximumRetries = maximumRetries.map(_.serialize)
    )
  }

}

object EmrCluster {

  def defaultEmrClusterFields(hc: HyperionContext) = EmrClusterFields(
    amiVersion = if (hc.emrReleaseLabel.nonEmpty) None else hc.emrAmiVersion,
    standardBootstrapAction = hc.emrEnvironmentUri.map(env => s"${hc.scriptUri}deploy-hyperion-emr-env.sh,$env": HString).toList,
    masterInstanceType = Option(hc.emrInstanceType: HString),
    coreInstanceCount = 2,
    coreInstanceType = Option(hc.emrInstanceType: HString),
    taskInstanceCount = 0,
    taskInstanceType = Option(hc.emrInstanceType: HString),
    releaseLabel = hc.emrReleaseLabel
  )

  def defaultResourceFields(hc: HyperionContext) = ResourceFields(
    keyPair = hc.emrKeyPair.map(x => x: HString),
    region = Option(hc.emrRegion: HString),
    availabilityZone = hc.emrAvailabilityZone.map(x => x: HString),
    resourceRole = Option(hc.emrResourceRole: HString),
    role = Option(hc.emrRole: HString),
    subnetId = hc.emrSubnetId.map(x => x: HString),
    terminateAfter = hc.emrTerminateAfter.map(x => x: HDuration),
    initTimeout = hc.emrInitTimeout.map(x => x: HDuration)
  )

}
