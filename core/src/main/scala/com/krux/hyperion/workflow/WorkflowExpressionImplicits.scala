package com.krux.hyperion.workflow

import scala.language.implicitConversions

import com.krux.hyperion.activity.PipelineActivity
import com.krux.hyperion.WorkflowExpression
import com.krux.hyperion.WorkflowActivityExpression

trait WorkflowExpressionImplicits {

  implicit def workflowIterable2WorkflowExpression(activities: Iterable[WorkflowExpression]): WorkflowExpression =
    activities.reduceLeft(_ + _)

  implicit def activityIterable2WorkflowExpression(activities: Iterable[PipelineActivity]): WorkflowExpression =
    activities.map(activity2WorkflowExpression).reduceLeft(_ + _)

  implicit def activity2WorkflowExpression(activity: PipelineActivity): WorkflowExpression =
    WorkflowActivityExpression(activity)

}
