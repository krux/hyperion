package com.krux.hyperion

import com.krux.hyperion.workflow.WorkflowExpression


/**
 * Workflows to be deployed to independent data pipelines in AWS. This is to mainly a workaround
 * for the limited number of objects and number of fields restriction by AWS. Also this make sure
 * tasks are distributed to multiple physical pipelines for management reasons.
 */
trait DataPipelineDefGroup[A] extends AbstractDataPipelineDef {

  /**
   * Function to determine the how workflow should be grouped together
   */
  def configGroups: Map[WorkflowKey, Iterable[A]]

  override def pipelineNames = configGroups.keySet
    .map(pipelineName + _.map(NameKeySeparator + _).getOrElse(""))

  /**
   * Defines the details of the workflow based on an iterable of configuration A
   */
  def groupWorkflow(group: Iterable[A]): WorkflowExpression

  final def workflows: Map[WorkflowKey, WorkflowExpression] = configGroups.map { case (key, xs) =>
    (key, groupWorkflow(xs))
  }

}
