package com.krux.hyperion

import com.krux.hyperion.expression.Parameter
import com.krux.hyperion.workflow.WorkflowExpression


case class DataPipelineDefGroupWrapper private (
  override val hc: HyperionContext,
  override val pipelineName: String,
  override val nameKeySeparator: String,
  schedule: Schedule,
  workflowsFunc: () => Map[WorkflowKey, WorkflowExpression],  // for delayed workfow execution
  override val tags: Map[String, Option[String]],
  override val parameters: Iterable[Parameter[_]]
) extends DataPipelineDefGroup {

  def withName(name: String) = copy(pipelineName = name)
  def withSchedule(schedule: Schedule) = copy(schedule = schedule)
  def withTags(tags: Map[String, Option[String]]) = copy(tags = this.tags ++ tags)
  def withParameters(parameters: Iterable[Parameter[_]]) = copy(parameters = parameters)

  def workflows = workflowsFunc()

}

object DataPipelineDefGroupWrapper {

  def apply(inner: DataPipelineDefGroup): DataPipelineDefGroupWrapper =
    new DataPipelineDefGroupWrapper(
      inner.hc,
      inner.pipelineName,
      inner.nameKeySeparator,
      inner.schedule,
      () => inner.workflows,
      inner.tags,
      inner.parameters
    )

}
