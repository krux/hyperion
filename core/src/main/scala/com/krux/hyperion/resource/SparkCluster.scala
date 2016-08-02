package com.krux.hyperion.resource

import org.slf4j.LoggerFactory

import com.krux.hyperion.adt.HString
import com.krux.hyperion.common.{ PipelineObjectId, BaseFields }
import com.krux.hyperion.HyperionContext

/**
 * Launch a Spark cluster
 */
case class SparkCluster private (
  baseFields: BaseFields,
  resourceFields: ResourceFields,
  emrClusterFields: EmrClusterFields,
  sparkVersion: Option[HString]
) extends EmrCluster {

  type Self = SparkCluster

  val logger = LoggerFactory.getLogger(SparkCluster.getClass)

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateResourceFields(fields: ResourceFields) = copy(resourceFields = fields)
  def updateEmrClusterFields(fields: EmrClusterFields) = copy(emrClusterFields = fields)

  def withSparkVersion(sparkVersion: HString) = copy(sparkVersion = Option(sparkVersion))

  override def applications = if (releaseLabel.nonEmpty)
    ("Spark": HString) +: super.applications
  else
    super.applications

  override def standardBootstrapAction = if (releaseLabel.nonEmpty)
    super.standardBootstrapAction
  else
    sparkVersion.map(version => s"s3://support.elasticmapreduce/spark/install-spark,-v,${version},-x": HString).toSeq ++ super.standardBootstrapAction

}

object SparkCluster {

  def apply()(implicit hc: HyperionContext): SparkCluster = new SparkCluster(
    baseFields = BaseFields(PipelineObjectId(SparkCluster.getClass)),
    resourceFields = EmrCluster.defaultResourceFields(hc),
    emrClusterFields = EmrCluster.defaultEmrClusterFields(hc),
    sparkVersion = hc.emrSparkVersion
  )

}
