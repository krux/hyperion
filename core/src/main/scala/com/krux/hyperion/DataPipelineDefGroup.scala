package com.krux.hyperion

import com.krux.hyperion.workflow.WorkflowExpression


/**
 * Workflows to be deployed to independent data pipelines in AWS. This is to mainly a workaround
 * for the limited number of objects and number of fields restriction by AWS. Also this make sure
 * tasks are distributed to multiple physical pipelines for management reasons.
 */
trait DataPipelineDefGroup[A] extends AbstractDataPipelineDef {

  /**
   * Configs is a collection of workflow parameters A, such that they can be treated as single
   * worklfow that can be grouped together
   */
  def configs: Iterable[A]

  /**
   * Function to determine the how workflow should be grouped together
   */
  def configGroups(xs: Iterable[A]): Map[WorkflowKey, Iterable[A]]

  /**
   * Defines the details of the workflow based on an iterable of configuration A
   */
  def groupWorkflow(group: Iterable[A]): WorkflowExpression

  final def workflows: Map[WorkflowKey, WorkflowExpression] = configGroups(configs)
    .map { case (key, xs) => (key, groupWorkflow(xs)) }

}
