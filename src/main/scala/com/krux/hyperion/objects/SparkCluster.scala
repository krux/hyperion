package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpEmrCluster
import com.krux.hyperion.HyperionContext

/**
 * Launch a Spark cluster
 */
case class SparkCluster private (
  id: PipelineObjectId,
  taskInstanceCount: Int,
  coreInstanceCount: Int,
  instanceType: String,
  amiVersion: String,
  sparkVersion: String,
  terminateAfter: String
)(
  implicit val hc: HyperionContext
) extends EmrCluster {

  assert(taskInstanceCount >= 0)

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))

  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def instanceCount = 1 + coreInstanceCount + taskInstanceCount

  val bootstrapAction = s"s3://support.elasticmapreduce/spark/install-spark,-v,$sparkVersion,-x" ::
      hc.emrEnvironmentUri.map(env => s"${hc.scriptUri}deploy-hyperion-emr-env.sh,$env").toList

  def withTaskInstanceCount(n: Int) = this.copy(taskInstanceCount = n)

  lazy val serialize = AdpEmrCluster(
    id = id,
    name = Some(id),
    bootstrapAction = bootstrapAction,
    amiVersion = Some(amiVersion),
    masterInstanceType = Some(instanceType),
    coreInstanceType = Some(instanceType),
    coreInstanceCount = Some(coreInstanceCount.toString),
    taskInstanceType = Some(instanceType),
    taskInstanceCount = Some(taskInstanceCount.toString),
    terminateAfter = terminateAfter,
    keyPair = keyPair
  )

}

object SparkCluster {

  def apply()(implicit hc: HyperionContext) = {
    new SparkCluster(
      id = PipelineObjectId("SparkCluster"),
      taskInstanceCount = 0,
      coreInstanceCount = 2,
      instanceType = hc.emrInstanceType,
      amiVersion = hc.emrAmiVersion,
      sparkVersion = hc.sparkVersion,
      terminateAfter = hc.emrTerminateAfter
    )
  }

}
