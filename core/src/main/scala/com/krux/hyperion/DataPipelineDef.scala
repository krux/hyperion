package com.krux.hyperion

import com.krux.hyperion.workflow.WorkflowExpression


/**
 * Base trait of all data pipeline definitions. All data pipelines needs to implement this trait
 */
trait DataPipelineDef extends DataPipelineDefGroup {

  /**
   * Workflow to be defined
   */
  def workflow: WorkflowExpression

  final def workflows = Map(EmptyKey -> workflow)

}
