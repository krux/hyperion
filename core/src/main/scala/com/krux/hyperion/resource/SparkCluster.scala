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
  isReleaseLabel4xx: Boolean

) extends EmrCluster {

  type Self = SparkCluster

  val logger = LoggerFactory.getLogger(SparkCluster.getClass)

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateResourceFields(fields: ResourceFields) = copy(resourceFields = fields)
  def updateEmrClusterFields(fields: EmrClusterFields) = copy(emrClusterFields = fields)

  def withSparkVersion(sparkVersion: HString) = copy(sparkVersion = sparkVersion)

  override def withReleaseLabel(label: HString): Self = {
    super.withReleaseLabel(label)
    if ((label.toString.length >= 6) && (label.toString.substring(4, 5).toInt >= 4)) {
      //Assuming emr-x.y.z, and return x as a number.
      this.copy(
        defaultApplications = Seq("Spark": HString),
        defaultBootstrapActions = Seq.empty[HString],
        isReleaseLabel4xx = true
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
    isReleaseLabel4xx = false
  )

}
