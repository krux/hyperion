package com.krux.hyperion.workflow

import com.krux.hyperion.activity.PipelineActivity
import com.krux.hyperion.common.PipelineObject
import com.krux.hyperion.resource.ResourceObject


sealed abstract class WorkflowExpression {

  def toActivities: Iterable[PipelineActivity[_ <: ResourceObject]] = {

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

  def toPipelineObjects: Iterable[PipelineObject] = {

    def flattenPipelineObjects(r: Map[String, PipelineObject], po: PipelineObject): Map[String, PipelineObject] =
      if (r.contains(po.id.toString)) r
      else r ++ Map(po.id.toString -> po) ++ po.objects.foldLeft(r)(flattenPipelineObjects)

    toActivities
      .foldLeft(Map.empty[String, PipelineObject])(flattenPipelineObjects)
      .values

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

object WorkflowExpression extends WorkflowExpressionImplicits {
  def empty: WorkflowExpression = WorkflowNoActivityExpression
}
