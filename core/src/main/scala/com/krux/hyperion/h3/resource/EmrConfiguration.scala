package com.krux.hyperion.h3.resource

import shapeless._

import com.krux.hyperion.aws.{ AdpEmrConfiguration, AdpRef }
import com.krux.hyperion.h3.common.{ ObjectFields, PipelineObjectId, PipelineObject }
import com.krux.hyperion.adt.HString

case class EmrConfiguration private (
  baseFields: ObjectFields,
  classification: Option[HString],
  properties: Seq[Property],
  configurations: Seq[EmrConfiguration]
) extends PipelineObject {

  type Self = EmrConfiguration

  def baseFieldsLens = lens[Self] >> 'baseFields

  def withClassification(classification: HString) =
    this.copy(classification = Option(classification))

  def withProperty(property: Property*) = this.copy(properties = this.properties ++ property)

  def withConfiguration(configuration: EmrConfiguration*) =
    this.copy(configurations = this.configurations ++ configuration)

  // def objects: Iterable[PipelineObject] = properties ++ configurations
  def objects: Iterable[PipelineObject] = None

  lazy val serialize = AdpEmrConfiguration(
    id = id,
    name = id.toOption,
    classification = classification.map(_.serialize),
    property = Option(properties.map(_.ref)),
    configuration = Option(configurations.map(_.ref))
  )

  def ref: AdpRef[AdpEmrConfiguration] = AdpRef(serialize)
}

object EmrConfiguration {

  def apply(property: Property*): EmrConfiguration = EmrConfiguration(
    baseFields = ObjectFields(PipelineObjectId(Property.getClass)),
    classification = None,
    properties = property,
    configurations = Seq.empty
  )

}
