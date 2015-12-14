package com.krux.hyperion.resource

import com.krux.hyperion.aws.{ AdpEmrConfiguration, AdpRef }
import com.krux.hyperion.common.{ ObjectFields, PipelineObjectId, PipelineObject, NamedPipelineObject }
import com.krux.hyperion.adt.HString

case class EmrConfiguration private (
  baseFields: ObjectFields,
  classification: Option[HString],
  properties: Seq[Property],
  configurations: Seq[EmrConfiguration]
) extends NamedPipelineObject {

  type Self = EmrConfiguration

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)

  def withClassification(classification: HString) =
    this.copy(classification = Option(classification))

  def withProperty(property: Property*) = this.copy(properties = this.properties ++ property)

  def withConfiguration(configuration: EmrConfiguration*) =
    this.copy(configurations = this.configurations ++ configuration)

  def objects = configurations ++ properties

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
