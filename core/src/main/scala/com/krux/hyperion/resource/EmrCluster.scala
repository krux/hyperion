package com.krux.hyperion.resource

import org.slf4j.LoggerFactory

import com.krux.hyperion.adt.HString
import com.krux.hyperion.common.{PipelineObjectId, BaseFields}
import com.krux.hyperion.HyperionContext


/**
 * Launch a EMR cluster later than release label 4.x.x
 */
case class EmrCluster private (
  baseFields: BaseFields,
  resourceFields: ResourceFields,
  emrClusterFields: EmrClusterFields
) extends BaseEmrCluster {

  type Self = EmrCluster

  val logger = LoggerFactory.getLogger(EmrCluster.getClass)

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateResourceFields(fields: ResourceFields) = copy(resourceFields = fields)
  def updateEmrClusterFields(fields: EmrClusterFields) = copy(emrClusterFields = fields)

  def withApplications(apps: EmrApplication*): Self = updateEmrClusterFields(
    emrClusterFields.copy(applications = emrClusterFields.applications ++ apps)
  )

  def withReleaseLabel(label: HString): Self = updateEmrClusterFields(
    emrClusterFields.copy(releaseLabel = Option(label), amiVersion = None)
  )

  def withConfiguration(conf: EmrConfiguration*): Self = updateEmrClusterFields(
    emrClusterFields.copy(configuration = emrClusterFields.configuration ++ conf)
  )

}

object EmrCluster {

  def apply()(implicit hc: HyperionContext): EmrCluster = new EmrCluster(
    baseFields = BaseFields(PipelineObjectId(EmrCluster.getClass)),
    resourceFields = BaseEmrCluster.defaultResourceFields(hc),
    emrClusterFields = EmrClusterFields(
      amiVersion = None,  // make sure ami version is not set (legacy only settings)
      standardBootstrapAction = Nil,  // legacy only settings
      masterInstanceType = Option(hc.emrInstanceType: HString),
      coreInstanceCount = 2,
      coreInstanceType = Option(hc.emrInstanceType: HString),
      taskInstanceCount = 0,
      taskInstanceType = Option(hc.emrInstanceType: HString),
      releaseLabel = hc.emrReleaseLabel,
      subnetId = hc.emrSubnetId
    )
  )

}
