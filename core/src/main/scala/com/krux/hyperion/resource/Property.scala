package com.krux.hyperion.resource

import com.krux.hyperion.aws.{AdpProperty, AdpRef}
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}

case class Property private (
  id: PipelineObjectId,
  key: Option[String],
  value: Option[String]
) extends PipelineObject {

  def objects: Iterable[PipelineObject] = None

  lazy val serialize = AdpProperty(
    id = id,
    name = id.toOption,
    key = key,
    value = value
  )

  def ref: AdpRef[AdpProperty] = AdpRef(serialize)
}

object Property {

  def apply(key: String, value: String): Property = Property(
    id = PipelineObjectId(Property.getClass),
    key = Option(key),
    value = Option(value)
  )

}
