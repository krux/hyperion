package com.krux.hyperion

import scala.language.implicitConversions

import com.krux.hyperion.activity.PipelineActivity

sealed abstract class WorkflowExpression {

  def toPipelineObjects: Iterable[PipelineActivity] = {

    def toPipelineObjectsRec(exp: WorkflowExpression): Set[PipelineActivity] = exp match {
      case WorkflowNoActivityExpression => Set()

      case WorkflowActivityExpression(activity) => Set(activity)

      case WorkflowArrowExpression(left, right) =>
        val leftDeps = toPipelineObjectsRec(left)
        val rightDeps = toPipelineObjectsRec(right)
        // rightDeps now should depend on leftDeps
        rightDeps.map(_.dependsOn(leftDeps.toSeq.sortBy(_.id): _*)) ++ leftDeps

      case WorkflowPlusExpression(left, right) =>
        val leftDeps = toPipelineObjectsRec(left)
        val rightDeps = toPipelineObjectsRec(right)
        (leftDeps ++ rightDeps).groupBy(_.id)
          .map { case (id, acts) =>
            acts.reduceLeft((a, b) => a.dependsOn(b.dependsOn: _*))
          }.toSet
    }

    toPipelineObjectsRec(this)
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

case class WorkflowActivityExpression(activity: PipelineActivity) extends WorkflowExpression

case class WorkflowArrowExpression(left: WorkflowExpression, right: WorkflowExpression) extends WorkflowExpression

case class WorkflowPlusExpression(left: WorkflowExpression, right: WorkflowExpression) extends WorkflowExpression

object WorkflowExpression {

  implicit def workflowIterable2WorkflowExpression(activities: Iterable[WorkflowExpression]): WorkflowExpression =
    activities.reduceLeft(_ + _)

  implicit def activityIterable2WorkflowExpression(activities: Iterable[PipelineActivity]): WorkflowExpression =
    activities.map(activity2WorkflowExpression).reduceLeft(_ + _)

  implicit def activity2WorkflowExpression(activity: PipelineActivity): WorkflowExpression =
    WorkflowActivityExpression(activity)

}
