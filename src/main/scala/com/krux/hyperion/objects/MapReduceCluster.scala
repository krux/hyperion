package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpEmrCluster, AdpJsonSerializer}
import com.krux.hyperion.HyperionContext

/**
 * Launch a MapReduce cluster
 */
case class MapReduceCluster private (
  id: UniquePipelineId,
  taskInstanceCount: Int
)(
  implicit val hc: HyperionContext
) extends EmrCluster {

  assert(taskInstanceCount >= 0)

  val amiVersion = hc.emrAmiVersion
  val coreInstanceCount = 2

  def instanceCount = 1 + coreInstanceCount + taskInstanceCount

  val bootstrapAction = hc.emrEnvironmentUri.map(
    env => s"${hc.scriptUri}deploy-hyperion-emr-env.sh,$env").toList

  val instanceType = hc.emrInstanceType

  val terminateAfter = hc.emrTerminateAfter

  def forClient(client: String) = this.copy(id = new UniquePipelineId(client))

  def withTaskInstanceCount(n: Int) = this.copy(taskInstanceCount = n)

  def serialize = AdpEmrCluster(
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

object MapReduceCluster {
  def apply()(implicit hc: HyperionContext) =
    new MapReduceCluster(
      id = new UniquePipelineId("MapReduceCluster"),
      taskInstanceCount = 0
    )
}
