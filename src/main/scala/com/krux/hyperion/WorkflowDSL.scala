package com.krux.hyperion

import com.krux.hyperion.activity.PipelineActivity
import scala.language.implicitConversions

/**
 * See com.krux.hyperion.examples.ExampleWorkflow for usage
 */
class WorkflowDSL(left: WorkflowExpression) {

  def andThen(right: WorkflowExpression): WorkflowExpression = WorkflowArrowExpression(left, right)
  def :~>(right: WorkflowExpression): WorkflowExpression = this.andThen(right)

  def priorTo_:(right: WorkflowExpression): WorkflowExpression = this.andThen(right)
  def <~:(right: WorkflowExpression): WorkflowExpression = this.priorTo_:(right)

  def and(right: WorkflowExpression): WorkflowExpression = WorkflowPlusExpression(left, right)
  def +(right: WorkflowExpression): WorkflowExpression = this.and(right)

}

object WorkflowDSL {

  implicit def activity2WorkflowDSL(act: PipelineActivity): WorkflowDSL =
    new WorkflowDSL(WorkflowActivityExpression(act))

  implicit def activity2WorkflowExpression(act: PipelineActivity): WorkflowExpression =
    WorkflowActivityExpression(act)

  implicit def expression2WorkflowDSL(exp: WorkflowExpression): WorkflowDSL =
    new WorkflowDSL(exp)

}
