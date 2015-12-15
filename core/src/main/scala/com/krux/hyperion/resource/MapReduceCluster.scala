package com.krux.hyperion.resource

import org.slf4j.LoggerFactory

import com.krux.hyperion.common.{ PipelineObjectId, BaseFields }
import com.krux.hyperion.HyperionContext

/**
 * Launch a map reduce cluster
 */
case class MapReduceCluster private (
  baseFields: BaseFields,
  resourceFields: ResourceFields,
  emrClusterFields: EmrClusterFields
) extends EmrCluster {

  type Self = MapReduceCluster

  val logger = LoggerFactory.getLogger(MapReduceCluster.getClass)

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateResourceFields(fields: ResourceFields) = copy(resourceFields = fields)
  def updateEmrClusterFields(fields: EmrClusterFields) = copy(emrClusterFields = fields)

}

object MapReduceCluster {

  def apply()(implicit hc: HyperionContext): MapReduceCluster = new MapReduceCluster(
    baseFields = BaseFields(PipelineObjectId(MapReduceCluster.getClass)),
    resourceFields = EmrCluster.defaultResourceFields(hc),
    emrClusterFields = EmrCluster.defaultEmrClusterFields(hc)
  )

}
