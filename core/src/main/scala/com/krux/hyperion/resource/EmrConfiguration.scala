package com.krux.hyperion.resource

import com.krux.hyperion.aws.{AdpEmrConfiguration, AdpRef}
import com.krux.hyperion.common.PipelineObject
import com.krux.hyperion.common.PipelineObjectId

case class EmrConfiguration private (
  id: PipelineObjectId,
  classification: Option[String],
  properties: Seq[Property],
  configurations: Seq[EmrConfiguration]
) extends PipelineObject {

  def withClassification(classification: String) = this.copy(classification = Option(classification))
  def withProperty(property: Property*) = this.copy(properties = this.properties ++ property)
  def withConfiguration(configuration: EmrConfiguration*) = this.copy(configurations = this.configurations ++ configuration)

  def objects: Iterable[PipelineObject] = properties ++ configurations

  lazy val serialize = AdpEmrConfiguration(
    id = id,
    name = id.toOption,
    classification = classification,
    property = Option(properties.map(_.ref)),
    configuration = Option(configurations.map(_.ref))
  )

  def ref: AdpRef[AdpEmrConfiguration] = AdpRef(serialize)
}

object EmrConfiguration {

  def apply(property: Property*): EmrConfiguration = EmrConfiguration(
    id = PipelineObjectId(Property.getClass),
    classification = None,
    properties = property,
    configurations = Seq.empty
  )

}
