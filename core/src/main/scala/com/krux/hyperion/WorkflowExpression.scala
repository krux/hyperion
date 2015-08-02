package com.krux.hyperion

import scala.language.implicitConversions

import com.krux.hyperion.activity.PipelineActivity

sealed abstract class WorkflowExpression {

  def toPipelineObjects: Iterable[PipelineActivity] = {

    def toPipelineObjectsRec(exp: WorkflowExpression): Set[PipelineActivity] =
      exp match {
        case WorkflowActivityExpression(act) => act

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
            }
            .toSet
      }

    toPipelineObjectsRec(this)
  }

  def andThen(right: WorkflowExpression): WorkflowExpression = WorkflowArrowExpression(this, right)
  def :~>(right: WorkflowExpression): WorkflowExpression = this.andThen(right)

  def priorTo_:(right: WorkflowExpression): WorkflowExpression = this.andThen(right)
  def <~:(right: WorkflowExpression): WorkflowExpression = this.priorTo_:(right)

  def and(right: WorkflowExpression): WorkflowExpression = WorkflowPlusExpression(this, right)
  def +(right: WorkflowExpression): WorkflowExpression = this.and(right)
}

case class WorkflowActivityExpression(activities: Set[PipelineActivity]) extends WorkflowExpression

case class WorkflowArrowExpression(left: WorkflowExpression, right: WorkflowExpression) extends WorkflowExpression

case class WorkflowPlusExpression(left: WorkflowExpression, right: WorkflowExpression) extends WorkflowExpression

object WorkflowExpression {

  implicit def activitySet2WorkflowExpression(activities: Set[PipelineActivity]): WorkflowExpression =
    WorkflowActivityExpression(activities)

  implicit def activityIterable2WorkflowExpression(activities: Iterable[PipelineActivity]): WorkflowExpression =
    WorkflowActivityExpression(activities.toSet)

  implicit def activitySeq2WorkflowExpression(activities: Seq[PipelineActivity]): WorkflowExpression =
    WorkflowActivityExpression(activities.toSet)

  implicit def activity2WorkflowExpression(activity: PipelineActivity): WorkflowExpression =
    WorkflowActivityExpression(Set(activity))

}
