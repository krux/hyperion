package com.krux.hyperion.h3.resource

import com.krux.hyperion.adt.{ HInt, HDouble, HString, HBoolean }

case class EmrClusterFields(
  amiVersion: Option[HString],
  supportedProducts: Option[HString],
  standardBootstrapAction: Seq[HString],
  bootstrapAction: Seq[HString],
  enableDebugging: Option[HBoolean],
  hadoopSchedulerType: Option[SchedulerType],
  coreInstanceBidPrice: Option[HDouble],
  coreInstanceCount: HInt,
  coreInstanceType: Option[HString],
  taskInstanceBidPrice: Option[HDouble],
  taskInstanceCount: HInt,
  taskInstanceType: Option[HString],
  masterSecurityGroupId: Option[HString],
  additionalMasterSecurityGroupIds: Seq[HString],
  slaveSecurityGroupId: Option[HString],
  additionalSlaveSecurityGroupIds: Seq[HString],
  visibleToAllUsers: Option[HBoolean],
  releaseLabel: Option[HString],  // do not use ami version with release label
  applications: Seq[HString],  // use with release label
  configuration: Option[EmrConfiguration]
)
