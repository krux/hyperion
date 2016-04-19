package com.krux.hyperion.resource

import com.krux.hyperion.aws.{ AdpEmrConfiguration, AdpRef }
import com.krux.hyperion.common.{ BaseFields, PipelineObjectId, NamedPipelineObject }
import com.krux.hyperion.adt.HString

case class EmrConfiguration private (
  baseFields: BaseFields,
  classification: Option[HString],
  properties: Seq[Property],
  configurations: Seq[EmrConfiguration]
) extends NamedPipelineObject {

  type Self = EmrConfiguration

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)

  def withClassification(classification: HString) = copy(classification = Option(classification))

  def withProperty(property: Property*) = copy(properties = this.properties ++ property)

  def withConfiguration(configuration: EmrConfiguration*) = copy(configurations = this.configurations ++ configuration)

  def objects = configurations ++ properties

  lazy val serialize = AdpEmrConfiguration(
    id = id,
    name = name,
    classification = classification.map(_.serialize),
    property = properties.map(_.ref),
    configuration = configurations.map(_.ref)
  )

  def ref: AdpRef[AdpEmrConfiguration] = AdpRef(serialize)
}

object EmrConfiguration {

  def apply(property: Property*): EmrConfiguration = EmrConfiguration(
    baseFields = BaseFields(PipelineObjectId(EmrConfiguration.getClass)),
    classification = None,
    properties = property,
    configurations = Seq.empty
  )

}
