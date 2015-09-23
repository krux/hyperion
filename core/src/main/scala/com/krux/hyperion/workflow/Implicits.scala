package com.krux.hyperion.workflow

import com.krux.hyperion.activity.PipelineActivity

import scala.language.implicitConversions

trait Implicits {

  implicit def workflowIterable2WorkflowExpression(activities: Iterable[WorkflowExpression]): WorkflowExpression =
    activities.reduceLeft(_ + _)

  implicit def activityIterable2WorkflowExpression(activities: Iterable[PipelineActivity]): WorkflowExpression =
    activities.map(activity2WorkflowExpression).reduceLeft(_ + _)

  implicit def activity2WorkflowExpression(activity: PipelineActivity): WorkflowExpression =
    WorkflowActivityExpression(activity)

}

object Implicits extends Implicits
