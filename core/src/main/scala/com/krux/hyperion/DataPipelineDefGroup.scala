package com.krux.hyperion

import com.krux.hyperion.workflow.WorkflowExpression

trait DataPipelineDefGroup[A] {

  def groupPool: Iterable[A]

  def groupSize: Int

  def workflowGroup(group: Iterable[A]): WorkflowExpression

  def workflows: Iterable[WorkflowExpression] = {
    ???
  }

}
