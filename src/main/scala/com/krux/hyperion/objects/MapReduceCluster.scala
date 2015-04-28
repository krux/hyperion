package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpEmrCluster
import com.krux.hyperion.HyperionContext

/**
 * Launch a MapReduce cluster
 */
case class MapReduceCluster private (
  id: PipelineObjectId,
  bootstrapAction: Seq[String],
  amiVersion: String,
  masterInstanceType: Option[String],
  coreInstanceType: Option[String],
  coreInstanceCount: Int,
  taskInstanceType: Option[String],
  taskInstanceCount: Int,
  taskInstanceBidPrice: Option[Double],
  terminateAfter: String,
  keyPair: Option[String],
  region: Option[String],
  enableDebugging: Option[Boolean],
  supportedProducts: Option[String],
  subnetId: Option[String]
)(
  implicit val hc: HyperionContext
) extends EmrCluster {

  assert(taskInstanceCount >= 0)
  assert(coreInstanceCount >= 1)

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))
  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def terminatingAfter(terminateAfter: String) = this.copy(terminateAfter = terminateAfter)
  def withAmiVersion(ver: String) = this.copy(amiVersion = ver)
  def withMasterInstanceType(instanceType: String) = this.copy(masterInstanceType = Option(instanceType))
  def withCoreInstanceType(instanceType: String) = this.copy(coreInstanceType = Option(instanceType))
  def withCoreInstanceCount(instanceCount: Int) = this.copy(coreInstanceCount = instanceCount)
  def withTaskInstanceType(instanceType: String) = this.copy(taskInstanceType = Option(instanceType))
  def withTaskInstanceCount(instanceCount: Int) = this.copy(taskInstanceCount = instanceCount)
  def withTaskInstanceBidPrice(bid: Double) = this.copy(taskInstanceBidPrice = Option(bid))
  def withKeyPair(keyPair: String) = this.copy(keyPair = Option(keyPair))
  def withRegion(region: String) = this.copy(region = Option(region))
  def withDebuggingEnabled() = this.copy(enableDebugging = Option(true))
  def withSupportedProducts(products: String) = this.copy(supportedProducts = Option(products))
  def withSubnetId(id: String) = this.copy(subnetId = Option(id))

  lazy val instanceCount = 1 + coreInstanceCount + taskInstanceCount

  lazy val standardBootstrapAction = hc.emrEnvironmentUri.map(env => s"${hc.scriptUri}deploy-hyperion-emr-env.sh,$env").toList

  lazy val serialize = AdpEmrCluster(
    id = id,
    name = id.toOption,
    bootstrapAction = standardBootstrapAction ++ bootstrapAction,
    amiVersion = Option(amiVersion),
    masterInstanceType = masterInstanceType,
    coreInstanceType = coreInstanceType,
    coreInstanceCount = Option(coreInstanceCount.toString),
    taskInstanceType = taskInstanceCount match {
      case 0 => None
      case _ => taskInstanceType
    },
    taskInstanceCount = taskInstanceCount match {
      case 0 => None
      case count => Option(count.toString)
    },
    taskInstanceBidPrice = taskInstanceCount match {
      case 0 => None
      case _ => taskInstanceBidPrice.map(_.toString)
    },
    terminateAfter = terminateAfter,
    keyPair = keyPair,
    region = region,
    enableDebugging = enableDebugging.map(_.toString),
    supportedProducts = supportedProducts,
    subnetId = subnetId
  )

}

object MapReduceCluster {
  def apply()(implicit hc: HyperionContext) = new MapReduceCluster(
    id = PipelineObjectId("MapReduceCluster"),
    bootstrapAction = Seq(),
    amiVersion = hc.emrAmiVersion,
    masterInstanceType = Option(hc.emrInstanceType),
    coreInstanceType = Option(hc.emrInstanceType),
    coreInstanceCount = 2,
    taskInstanceType = Option(hc.emrInstanceType),
    taskInstanceCount = 0,
    taskInstanceBidPrice = None,
    terminateAfter = hc.emrTerminateAfter,
    keyPair = hc.keyPair,
    region = Option(hc.region),
    enableDebugging = None,
    supportedProducts = None,
    subnetId = hc.subnetId
  )
}
