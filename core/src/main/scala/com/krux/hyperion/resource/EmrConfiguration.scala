package com.krux.hyperion.resource

import com.krux.hyperion.aws.{AdpEmrConfiguration, AdpRef}
import com.krux.hyperion.common.{BaseFields, PipelineObjectId, NamedPipelineObject}


case class EmrConfiguration private (
  baseFields: BaseFields,
  classification: Option[String],
  properties: Seq[Property],
  configurations: Seq[EmrConfiguration]
) extends NamedPipelineObject {

  type Self = EmrConfiguration

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)

  def withClassification(classification: String) = copy(classification = Option(classification))

  def withProperty(property: Property*) = copy(properties = this.properties ++ property)

  def withConfiguration(configuration: EmrConfiguration*) = copy(configurations = this.configurations ++ configuration)

  def objects = configurations ++ properties

  lazy val serialize = AdpEmrConfiguration(
    id = id,
    name = name,
    classification = classification,
    property = properties.map(_.ref),
    configuration = configurations.map(_.ref)
  )

  def ref: AdpRef[AdpEmrConfiguration] = AdpRef(serialize)
}

object EmrConfiguration {

  @deprecated("Use apply(classification: String) instead", "5.0.0")
  def apply(): EmrConfiguration = EmrConfiguration(
    baseFields = BaseFields(PipelineObjectId(EmrConfiguration.getClass)),
    classification = None,
    properties = Seq.empty,
    configurations = Seq.empty
  )

  def apply(classification: String): EmrConfiguration = EmrConfiguration(
    baseFields = BaseFields(PipelineObjectId(EmrConfiguration.getClass)),
    classification = Option(classification),
    properties = Seq.empty,
    configurations = Seq.empty
  )

}
