package com.krux.hyperion.resource

import org.slf4j.LoggerFactory

import com.krux.hyperion.adt.HString
import com.krux.hyperion.common.{PipelineObjectId, BaseFields}
import com.krux.hyperion.HyperionContext


/**
 * Use AMI versions to launch EMR clusters. Use EmrCluster for release label 4.x.x or later
 */
case class LegacyEmrCluster private(
  baseFields: BaseFields,
  resourceFields: ResourceFields,
  emrClusterFields: EmrClusterFields
) extends BaseEmrCluster {

  type Self = LegacyEmrCluster

  val logger = LoggerFactory.getLogger(LegacyEmrCluster.getClass)

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateResourceFields(fields: ResourceFields) = copy(resourceFields = fields)
  def updateEmrClusterFields(fields: EmrClusterFields) = copy(emrClusterFields = fields)

  def withAmiVersion(version: HString): Self = updateEmrClusterFields(
    emrClusterFields.copy(amiVersion = Option(version), releaseLabel = None)
  )

}

object LegacyEmrCluster {

  def apply()(implicit hc: HyperionContext): LegacyEmrCluster = new LegacyEmrCluster(
    baseFields = BaseFields(PipelineObjectId(EmrCluster.getClass)),
    resourceFields = BaseEmrCluster.defaultResourceFields(hc),
    emrClusterFields = LegacyEmrCluster.defaultEmrClusterFields
  )

  def defaultEmrClusterFields(implicit hc: HyperionContext): EmrClusterFields =
    EmrClusterFields(
      amiVersion = hc.emrAmiVersion,
      standardBootstrapAction = hc.emrEnvironmentUri.map(env => s"${hc.scriptUri}deploy-hyperion-emr-env.sh,$env": HString).toList,
      masterInstanceType = Option(hc.emrInstanceType: HString),
      coreInstanceCount = 2,
      coreInstanceType = Option(hc.emrInstanceType: HString),
      taskInstanceCount = 0,
      taskInstanceType = Option(hc.emrInstanceType: HString),
      releaseLabel = None  // make sure release label is not set
    )

}
