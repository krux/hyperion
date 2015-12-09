package com.krux.hyperion.h3.resource

import com.krux.hyperion.adt.{ HString, HBoolean, HDuration }
import com.krux.hyperion.h3.common.HttpProxy

case class ResourceFields(
  role: Option[HString] = None,
  resourceRole: Option[HString] = None,
  keyPair: Option[HString] = None,
  region: Option[HString] = None,
  availabilityZone: Option[HString] = None,
  subnetId: Option[HString] = None,
  useOnDemandOnLastAttempt: Option[HBoolean] = None,
  initTimeout: Option[HDuration] = None,
  terminateAfter: Option[HDuration] = None,
  actionOnResourceFailure: Option[ActionOnResourceFailure] = None,
  actionOnTaskFailure: Option[ActionOnTaskFailure] = None,
  httpProxy: Option[HttpProxy] = None
)


