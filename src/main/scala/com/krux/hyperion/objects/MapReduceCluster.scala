package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpEmrCluster, AdpJsonSerializer}
import com.krux.hyperion.HyperionContext

/**
 * Launch a MapReduce cluster
 */
case class MapReduceCluster private (
  id: PipelineObjectId,
  taskInstanceCount: Int
)(
  implicit val hc: HyperionContext
) extends EmrCluster {

  assert(taskInstanceCount >= 0)

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))

  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  val amiVersion = hc.emrAmiVersion
  val coreInstanceCount = 2

  def instanceCount = 1 + coreInstanceCount + taskInstanceCount

  val bootstrapAction = hc.emrEnvironmentUri.map(
    env => s"${hc.scriptUri}deploy-hyperion-emr-env.sh,$env").toList

  val instanceType = hc.emrInstanceType

  val terminateAfter = hc.emrTerminateAfter

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
      id = PipelineObjectId("MapReduceCluster"),
      taskInstanceCount = 0
    )
}
