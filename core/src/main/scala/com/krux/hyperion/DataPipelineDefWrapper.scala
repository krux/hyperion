package com.krux.hyperion

import com.krux.hyperion.expression.{Parameter, ParameterValues}

/**
  * DataPipelineDefWrapper provides a way to wrap other DataPipelineDefs
  * in order to override aspects.
  */
private[hyperion] case class DataPipelineDefWrapper(
  override val pipelineName: String,
  schedule: Schedule,
  workflow: WorkflowExpression,
  override val tags: Map[String, Option[String]],
  override val parameters: Iterable[Parameter[_]],
  override implicit val pv: ParameterValues
) extends DataPipelineDef {

  def withName(name: String) = this.copy(pipelineName = name)
  def withSchedule(schedule: Schedule) = this.copy(schedule = schedule)
  def withTags(tags: Map[String, Option[String]]) = this.copy(tags = this.tags ++ tags)
  def withParameters(parameters: Iterable[Parameter[_]]) = this.copy(parameters = parameters)
  def withParameterValues(pv: ParameterValues) = this.copy(pv = pv)

}

object DataPipelineDefWrapper {
  def apply(inner: DataPipelineDef): DataPipelineDefWrapper = DataPipelineDefWrapper(
    inner.pipelineName,
    inner.schedule,
    inner.workflow,
    inner.tags,
    inner.parameters,
    inner.pv
  )
}
