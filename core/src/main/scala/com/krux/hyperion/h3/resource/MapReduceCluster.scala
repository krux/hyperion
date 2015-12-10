package com.krux.hyperion.h3.resource

import shapeless._
import org.slf4j.LoggerFactory

import com.krux.hyperion.adt.HType._
import com.krux.hyperion.adt.{ HInt, HDuration, HDouble, HString, HBoolean }
import com.krux.hyperion.aws.AdpEmrCluster
import com.krux.hyperion.h3.common.{ HttpProxy, PipelineObject, PipelineObjectId, ObjectFields }
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

  def baseFieldsLens = lens[Self] >> 'baseFields
  def resourceFieldsLens = lens[Self] >> 'resourceFields
  def emrClusterFieldsLens = lens[Self] >> 'emrClusterFields

  override def objects = None
  // override def objects: Iterable[PipelineObject] = configuration.toList ++ httpProxy.toList

}

object MapReduceCluster {

  def apply()(implicit hc: HyperionContext): MapReduceCluster = apply(None)

  def apply(configuration: EmrConfiguration)(implicit hc: HyperionContext): MapReduceCluster =
    apply(Option(configuration))

  def defaultResourceFields(hc: HyperionContext) = ResourceFields(
    keyPair = hc.emrKeyPair.map(x => x: HString),
    region = Option(hc.emrRegion: HString),
    availabilityZone = hc.emrAvailabilityZone.map(x => x: HString),
    resourceRole = Option(hc.emrResourceRole: HString),
    role = Option(hc.emrRole: HString),
    subnetId = hc.emrSubnetId.map(x => x: HString),
    terminateAfter = hc.emrTerminateAfter.map(x => x: HDuration)
  )

  def defaultEmrClusterFields(hc: HyperionContext) = EmrClusterFields(
    amiVersion = hc.emrAmiVersion,
    standardBootstrapAction = hc.emrEnvironmentUri.map(env => s"${hc.scriptUri}deploy-hyperion-emr-env.sh,$env": HString).toList,
    masterInstanceType = Option(hc.emrInstanceType: HString),
    coreInstanceCount = 2,
    coreInstanceType = Option(hc.emrInstanceType: HString),
    taskInstanceCount = 0,
    taskInstanceType = Option(hc.emrInstanceType: HString),
    releaseLabel = hc.emrReleaseLabel
  )

  private def apply(configuration: Option[EmrConfiguration])(implicit hc: HyperionContext): MapReduceCluster = new MapReduceCluster(
    baseFields = ObjectFields(PipelineObjectId(MapReduceCluster.getClass)),
    resourceFields = defaultResourceFields(hc),
    emrClusterFields = defaultEmrClusterFields(hc)
  ).withConfiguration(configuration.get)
}
