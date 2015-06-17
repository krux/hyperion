package com.krux.hyperion

import com.krux.hyperion.activity.PipelineActivity
import scala.language.implicitConversions

/**
 * See com.krux.hyperion.examples.ExampleWorkflow for usage
 */
class WorkflowDSL(activities: Seq[PipelineActivity]) {

  // assert that we are not mix the dependsOn approach with WorkFlowDSL, it will have unexpected
  // behavior
  private def assertNoDependencies(acts: PipelineActivity*) = {
    for (act <- acts) assert(act.dependsOn.isEmpty)
  }

  def ~>(act: PipelineActivity): Seq[PipelineActivity] = {
    assertNoDependencies(act)
    Seq(act.dependsOn(activities:_*))
  }

  def andThen(act: PipelineActivity): Seq[PipelineActivity] = this.~>(act)

  def ~>(acts: Seq[PipelineActivity]): Seq[PipelineActivity] = {
    assertNoDependencies(acts:_*)
    for (act <- acts) yield act.dependsOn(activities:_*)
  }

  def andThen(acts: Seq[PipelineActivity]): Seq[PipelineActivity] = this.~>(acts)

  def +(act: PipelineActivity): Seq[PipelineActivity] = activities :+ act

  def and(act: PipelineActivity): Seq[PipelineActivity] = this.+(act)

  def +(acts: Seq[PipelineActivity]): Seq[PipelineActivity] = activities ++ acts

  def and(acts: Seq[PipelineActivity]): Seq[PipelineActivity] = this.+(acts)


}

object WorkflowDSL {

  implicit def activity2WorkFlowDSL(act: PipelineActivity): WorkflowDSL =
    new WorkflowDSL(Seq(act))

  implicit def activities2WorkFlowDSL(acts: Iterable[PipelineActivity]): WorkflowDSL =
    new WorkflowDSL(acts.toSeq)

}
