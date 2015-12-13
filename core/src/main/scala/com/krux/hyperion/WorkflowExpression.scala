package com.krux.hyperion

import scala.language.existentials

import com.krux.hyperion.activity.PipelineActivity
import com.krux.hyperion.workflow.WorkflowExpressionImplicits
import com.krux.hyperion.workflow.WorkflowGraph
import com.krux.hyperion.resource.ResourceObject

sealed abstract class WorkflowExpression {

  def toPipelineObjects: Iterable[PipelineActivity[_]] = {

    def toWorkflowGraph(exp: WorkflowExpression): WorkflowGraph = {
      exp match {
        case WorkflowNoActivityExpression =>
          new WorkflowGraph()
        case WorkflowActivityExpression(act) =>
          new WorkflowGraph(act)
        case WorkflowArrowExpression(left, right) =>
          toWorkflowGraph(left) ~> toWorkflowGraph(right)
        case WorkflowPlusExpression(left, right) =>
          toWorkflowGraph(left) ++ toWorkflowGraph(right)
      }
    }

    toWorkflowGraph(this).toActivities
  }

  def andThen(right: WorkflowExpression): WorkflowExpression = right match {
    case WorkflowNoActivityExpression => this
    case r if this == WorkflowNoActivityExpression => r
    case r => WorkflowArrowExpression(this, r)
  }

  def ~>(right: WorkflowExpression): WorkflowExpression = this.andThen(right)

  def and(right: WorkflowExpression): WorkflowExpression = right match {
    case WorkflowNoActivityExpression => this
    case r if this == WorkflowNoActivityExpression => r
    case r => WorkflowPlusExpression(this, right)
  }

  def +(right: WorkflowExpression): WorkflowExpression = this.and(right)
}

case object WorkflowNoActivityExpression extends WorkflowExpression

case class WorkflowActivityExpression(activity: PipelineActivity[_ <: ResourceObject]) extends WorkflowExpression

case class WorkflowArrowExpression(left: WorkflowExpression, right: WorkflowExpression) extends WorkflowExpression

case class WorkflowPlusExpression(left: WorkflowExpression, right: WorkflowExpression) extends WorkflowExpression

object WorkflowExpression extends WorkflowExpressionImplicits
