package com.krux.hyperion

import com.krux.hyperion.activity.PipelineActivity
import com.krux.hyperion.common.PipelineObject

sealed abstract class WorkflowExpression {

  def toPipelineObjects: Iterable[PipelineActivity] = {

    def toPipelineObjectsRec(exp: WorkflowExpression): Set[PipelineActivity] =
      exp match {
        case WorkflowActivityExpression(act) => Set(act)

        case WorkflowArrowExpression(left, right) =>
          val leftDeps = toPipelineObjectsRec(left)
          val rightDeps = toPipelineObjectsRec(right)
          // rightDeps now should depend on leftDeps
          rightDeps.map(_.dependsOn(leftDeps.toSeq:_*)) ++ leftDeps

        case WorkflowPlusExpression(left, right) =>
          val leftDeps = toPipelineObjectsRec(left)
          val rightDeps = toPipelineObjectsRec(right)
          (leftDeps ++ rightDeps).groupBy(_.id)
            .map { case (id, acts) =>
              acts.reduceLeft((a, b) => a.dependsOn(b.dependsOn:_*))
            }
            .toSet
      }

    toPipelineObjectsRec(this)
  }

}

case class WorkflowActivityExpression(act: PipelineActivity) extends WorkflowExpression

case class WorkflowArrowExpression(left: WorkflowExpression, right: WorkflowExpression) extends WorkflowExpression

case class WorkflowPlusExpression(left: WorkflowExpression, right: WorkflowExpression) extends WorkflowExpression
