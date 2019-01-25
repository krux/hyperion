package com.krux.hyperion.resource

import com.krux.hyperion.adt.{HBoolean, HInt}
import com.krux.hyperion.aws._
import com.krux.hyperion.common.{BaseFields, NamedPipelineObject, PipelineObjectId}
import com.krux.hyperion.resource.EmrVolumeSpecification.VolumeType

/**
 * EBS volume configuration for EMR cluster nodes
 * https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/emrcluster-example-ebs.html
 */
case class EmrEbsConfiguration private (
  baseFields: BaseFields,
  ebsOptimized: Option[HBoolean],
  ebsBlockDeviceConfig: Option[EmrEbsBlockDeviceConfig]
) extends NamedPipelineObject {

  type Self = EmrEbsConfiguration

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)

  def objects = ebsBlockDeviceConfig

  lazy val serialize = AdpEmrEbsConfiguration(
    id,
    name,
    ebsOptimized = ebsOptimized.map(_.serialize),
    ebsBlockDeviceConfig = ebsBlockDeviceConfig.map(_.ref)
  )

  def ref: AdpRef[AdpEmrEbsConfiguration] = AdpRef(serialize)
}

object EmrEbsConfiguration {

  def apply(ebsOptimized: HBoolean, ebsBlockDeviceConfig: EmrEbsBlockDeviceConfig): EmrEbsConfiguration = EmrEbsConfiguration(
    baseFields = BaseFields(PipelineObjectId(EmrEbsConfiguration.getClass)),
    ebsOptimized = Option(ebsOptimized),
    ebsBlockDeviceConfig = Option(ebsBlockDeviceConfig)
  )
}


case class EmrEbsBlockDeviceConfig private (
  baseFields: BaseFields,
  volumesPerInstance: Option[HInt],
  volumeSpecification: Option[EmrVolumeSpecification]
) extends NamedPipelineObject {

  type Self = EmrEbsBlockDeviceConfig

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)

  def objects = volumeSpecification

  lazy val serialize = AdpEmrEbsBlockDeviceConfig(
    id = id,
    name = name,
    volumesPerInstance = volumesPerInstance.map(_.serialize),
    volumeSpecification = volumeSpecification.map(_.ref)
  )

  def ref: AdpRef[AdpEmrEbsBlockDeviceConfig] = AdpRef(serialize)
}

object EmrEbsBlockDeviceConfig {

  def apply(volumesPerInstance: HInt, volumeSpecification: EmrVolumeSpecification): EmrEbsBlockDeviceConfig = EmrEbsBlockDeviceConfig(
    baseFields = BaseFields(PipelineObjectId(EmrEbsBlockDeviceConfig.getClass)),
    volumesPerInstance = Option(volumesPerInstance),
    volumeSpecification = Option(volumeSpecification)
  )
}

case class EmrVolumeSpecification private (
  baseFields: BaseFields,
  sizeInGB: Option[HInt],
  volumeType: Option[VolumeType],
  iops: Option[HInt]
) extends NamedPipelineObject {

  type Self = EmrVolumeSpecification

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)

  def objects = None

  lazy val serialize = AdpEmrVolumeSpecification(
    id = id,
    name = name,
    sizeInGB = sizeInGB.map(_.serialize),
    volumeType = volumeType.map(_.serialize),
    iops = iops.map(_.serialize)
  )

  def ref: AdpRef[AdpEmrVolumeSpecification] = AdpRef(serialize)
}

object EmrVolumeSpecification {
  /**
  * https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumeTypes.html
  */
  sealed abstract class VolumeType(val serialize: String) {
    override def toString = serialize
  }

  case object GeneralPurposeSSD extends VolumeType("gp2")
  case object ProvisionedIopsSSD  extends VolumeType("io1")
  case object ThroughputOptimizedHDD  extends VolumeType("st1")
  case object ColdHDD extends VolumeType("sc1")

  def apply(sizeInGB: HInt, volumeType: VolumeType, iops: HInt): EmrVolumeSpecification = EmrVolumeSpecification(
    baseFields = BaseFields(PipelineObjectId(EmrVolumeSpecification.getClass)),
    sizeInGB = Option(sizeInGB),
    volumeType = Option(volumeType),
    iops = Option(iops)
  )

  def apply(sizeInGB: HInt, volumeType: VolumeType): EmrVolumeSpecification = EmrVolumeSpecification(
    baseFields = BaseFields(PipelineObjectId(EmrVolumeSpecification.getClass)),
    sizeInGB = Option(sizeInGB),
    volumeType = Option(volumeType),
    iops = None
  )
}


