package com.krux.hyperion.resource

import com.krux.hyperion.common.{PipelineObjectId, BaseFields}
import com.krux.hyperion.HyperionContext


case class LegacyEmrCluster private(
  baseFields: BaseFields,
  resourceFields: ResourceFields,
  emrClusterFields: EmrClusterFields
) extends BaseEmrCluster {

  type Self = LegacyEmrCluster

  val logger = LoggerFactory.getLogger(LegacyEmrCluster.getClass)

}

object LegacyEmrCluster {
}
