package com.krux.hyperion.common

trait NamedPipelineObject extends PipelineObject {

  type Self <: NamedPipelineObject

  def baseFields: ObjectFields
  def updateBaseFields(fields: ObjectFields): Self

  def id = baseFields.id
  def named(name: String) = updateBaseFields(
    baseFields.copy(id = baseFields.id.named(name))
  )
  def groupedBy(group: String) = updateBaseFields(
    baseFields.copy(id = baseFields.id.groupedBy(group))
  )

}
