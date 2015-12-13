package com.krux.hyperion.resource

import org.slf4j.LoggerFactory

import com.krux.hyperion.adt.HType._
import com.krux.hyperion.common.{ PipelineObjectId, ObjectFields }
import com.krux.hyperion.HyperionContext

/**
 * Launch a map reduce cluster
 */
case class MapReduceCluster private (
  baseFields: ObjectFields,
  resourceFields: ResourceFields,
  emrClusterFields: EmrClusterFields
) extends EmrCluster {

  type Self = MapReduceCluster

  val logger = LoggerFactory.getLogger(MapReduceCluster.getClass)

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateResourceFields(fields: ResourceFields) = copy(resourceFields = fields)
  def updateEmrClusterFields(fields: EmrClusterFields) = copy(emrClusterFields = fields)

  override def objects = None
  // override def objects: Iterable[PipelineObject] = configuration.toList ++ httpProxy.toList

}

object MapReduceCluster {

  def apply()(implicit hc: HyperionContext): MapReduceCluster = new MapReduceCluster(
    baseFields = ObjectFields(PipelineObjectId(MapReduceCluster.getClass)),
    resourceFields = EmrCluster.defaultResourceFields(hc),
    emrClusterFields = EmrCluster.defaultEmrClusterFields(hc)
  )

}
