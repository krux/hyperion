package com.krux.hyperion

import com.krux.hyperion.activity.PipelineActivity
import scala.language.implicitConversions

/**
 * See com.krux.hyperion.examples.ExampleWorkflow for usage
 */
class WorkflowDSL(activities: Seq[PipelineActivity]) {

  def andThen(act: PipelineActivity): Seq[PipelineActivity] = Seq(act.dependsOn(activities:_*))

  def :~>(act: PipelineActivity): Seq[PipelineActivity] = this.andThen(act)

  def priorTo_:(act: PipelineActivity): Seq[PipelineActivity] = this.andThen(act)

  def <~:(act: PipelineActivity): Seq[PipelineActivity] = this.priorTo_:(act)

  def andThen(acts: Seq[PipelineActivity]): Seq[PipelineActivity] = acts.map(_.dependsOn(activities:_*))

  def :~>(acts: Seq[PipelineActivity]): Seq[PipelineActivity] = this.andThen(acts)

  def priorTo_:(acts: Seq[PipelineActivity]): Seq[PipelineActivity] = this.andThen(acts)

  def <~:(acts: Seq[PipelineActivity]): Seq[PipelineActivity] = this.priorTo_:(acts)

  def and(act: PipelineActivity): Seq[PipelineActivity] = activities :+ act

  def +(act: PipelineActivity): Seq[PipelineActivity] = this.and(act)

  def and(acts: Seq[PipelineActivity]): Seq[PipelineActivity] = activities ++ acts

  def +(acts: Seq[PipelineActivity]): Seq[PipelineActivity] = this.and(acts)

}

object WorkflowDSL {

  implicit def activity2WorkFlowDSL(act: PipelineActivity): WorkflowDSL =
    new WorkflowDSL(Seq(act))

  implicit def activities2WorkFlowDSL(acts: Iterable[PipelineActivity]): WorkflowDSL =
    new WorkflowDSL(acts.toSeq)

}
