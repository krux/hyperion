package com.krux.hyperion.resource

import com.krux.hyperion.aws.{AdpRef, AdpEmrCluster}

trait EmrCluster extends ResourceObject {

  def serialize: AdpEmrCluster

  def ref: AdpRef[AdpEmrCluster] = AdpRef(serialize)

}
