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
  sparkVersion: HString,
  defaultApplications: Seq[HString],
  defaultBootstrapActions: Seq[HString],
  hasReleaseLabel: Boolean

) extends EmrCluster {

  type Self = SparkCluster

  val logger = LoggerFactory.getLogger(SparkCluster.getClass)

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateResourceFields(fields: ResourceFields) = copy(resourceFields = fields)
  def updateEmrClusterFields(fields: EmrClusterFields) = copy(emrClusterFields = fields)

  def withSparkVersion(sparkVersion: HString) = copy(sparkVersion = sparkVersion)

  override def withReleaseLabel(label: HString): Self = {
    val newCopy = super.withReleaseLabel(label)
    if (newCopy.releaseLabel.nonEmpty) {
      newCopy.copy(
        defaultApplications = Seq("Spark": HString),
        defaultBootstrapActions = Seq.empty[HString],
          hasReleaseLabel = true
      )
    } else {
      this
    }
  }

  override def applications =
    this.defaultApplications ++ super.applications

  override def standardBootstrapAction = 
    this.defaultBootstrapActions ++ super.standardBootstrapAction

}

object SparkCluster {

  def apply()(implicit hc: HyperionContext): SparkCluster = new SparkCluster(
    baseFields = BaseFields(PipelineObjectId(SparkCluster.getClass)),
    resourceFields = EmrCluster.defaultResourceFields(hc),
    emrClusterFields = EmrCluster.defaultEmrClusterFields(hc),
    sparkVersion = hc.emrSparkVersion.get,
    defaultApplications = Seq.empty[HString],
    defaultBootstrapActions = Seq(s"s3://support.elasticmapreduce/spark/install-spark,-v,${hc.emrSparkVersion.get},-x": HString),
      hasReleaseLabel = false
  )

}
