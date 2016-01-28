package com.krux.hyperion.workflow

import scala.annotation.tailrec
import scala.language.implicitConversions

import com.krux.hyperion.activity.PipelineActivity
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.resource.ResourceObject

class WorkflowGraph private (
  val flow: Map[PipelineObjectId, Set[PipelineObjectId]],
  val activities: Map[PipelineObjectId, PipelineActivity[_ <: ResourceObject]]
) {

  type Flow = Map[PipelineObjectId, Set[PipelineObjectId]]

  def this() =
    this(
      Map.empty[PipelineObjectId, Set[PipelineObjectId]],
      Map.empty[PipelineObjectId, PipelineActivity[_ <: ResourceObject]]
    )

  def this(act: PipelineActivity[_ <: ResourceObject]) = {
    this(
      Map.empty[PipelineObjectId, Set[PipelineObjectId]],
      Map(act.id -> act)
    )
  }

  /**
   * isolates are both roots and leaves
   */
  lazy val isolates: Set[PipelineObjectId] = activities.keySet -- flow.keySet

  /**
   * Ids of PipelineObject that does not depend on anything
   */
  lazy val roots: Set[PipelineObjectId] =
    flow.values.foldLeft(flow.keySet) { (rs, dependents) =>
      rs -- dependents
    } ++ isolates

  /**
   * Ids of the PipelineObject have nothing depend on them
   */
  lazy val leaves: Set[PipelineObjectId] =
    (flow.values.flatten.toSet -- flow.keySet) ++ isolates

  /**
   * Add a single activity
   */
  def +(act: PipelineActivity[_ <: ResourceObject]) =
    if (activities.contains(act.id)) this
    else new WorkflowGraph(flow, activities + (act.id -> act))

  def +(act1: PipelineActivity[_ <: ResourceObject], act2: PipelineActivity[_ <: ResourceObject]) = {
    val dependents = flow.get(act1.id) match {
      case Some(acts) => acts + act2.id
      case None => Set(act2.id)
    }

    val newActivities = activities + (act1.id -> act1) + (act2.id -> act2)

    new WorkflowGraph(flow + (act1.id -> dependents), newActivities)
  }

  /**
   * The other graph follows this graph
   */
  def ~>(other: WorkflowGraph): WorkflowGraph = {
    val leafToRoots = this.leaves.map { r => r -> other.roots }.toMap

    val newFlow = Seq(this.flow, other.flow, leafToRoots).reduceLeft(mergeFlow)
    val newActivities = this.activities ++ other.activities

    new WorkflowGraph(newFlow, newActivities)
  }

  /**
   * Merges two workflow graphs
   */
  def ++(other: WorkflowGraph): WorkflowGraph = {
    val newFlow = mergeFlow(this.flow, other.flow)
    val newActivities = this.activities ++ other.activities
    new WorkflowGraph(newFlow, newActivities)
  }

  private def mergeFlow(flow1: Flow, flow2: Flow): Flow =
    flow2.foldLeft(flow1) { case (f, (act, dependents)) =>
      val newDependents = f.get(act) match {
        case Some(ds) => ds ++ dependents
        case None => dependents
      }
      f + (act -> newDependents)
    }

  implicit def pipelineId2Activity(pId: PipelineObjectId): PipelineActivity[_ <: ResourceObject] = activities(pId)

  @tailrec
  final def toActivities: Iterable[PipelineActivity[_ <: ResourceObject]] = {
    assert(roots.nonEmpty)
    assert(leaves.nonEmpty)

    if (flow.isEmpty) {
      activities.values
    } else {
      // get the immediate dependencies from the root node
      val rootDependents: Set[(PipelineObjectId, PipelineObjectId)] =
        for {
          act <- roots -- isolates
          dependent <- flow(act)
        } yield (dependent, act)

      // assign dependees to the immediate dependents
      val actsWithDeps = rootDependents.groupBy(_._1)
        .map { case (dependent, group) =>
          dependent.dependsOn(group.map(_._2).toSeq.map(activities).sortBy(_.id): _*)
        }

      val newActivities = actsWithDeps.foldLeft(activities)((acts, act) => acts + (act.id -> act))
      new WorkflowGraph(flow -- (roots -- isolates), newActivities).toActivities
    }
  }

}
