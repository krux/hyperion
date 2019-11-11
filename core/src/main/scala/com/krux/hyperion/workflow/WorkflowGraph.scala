package com.krux.hyperion.workflow

import com.krux.hyperion.activity.PipelineActivity
import com.krux.hyperion.resource.ResourceObject

class WorkflowGraph private (
  val nodes: Seq[PipelineActivity[_ <: ResourceObject]],
  val roots: Seq[PipelineActivity[_ <: ResourceObject]],
  val leaves: Seq[PipelineActivity[_ <: ResourceObject]],
  val dependencies: Seq[(PipelineActivity[_ <: ResourceObject], PipelineActivity[_ <: ResourceObject])]
) {
  type A = PipelineActivity[_ <: ResourceObject]

  /**
   * The other graph of the expression follows this graph
   */
  def ~>(other: WorkflowGraph): WorkflowGraph =
    WorkflowGraph(nodes ++ other.nodes, roots, other.leaves, dependencies ++ other.dependencies ++ other.roots.flatMap(r => leaves.map(l => r -> l)))

  /**
   * Merges two workflow graphs
   */
  def ++(other: WorkflowGraph): WorkflowGraph =
      WorkflowGraph(nodes ++ other.nodes, roots ++ other.roots, leaves ++ other.leaves, dependencies ++ other.dependencies)


  private lazy val dependencyGraph: Map[A, Seq[A]] =
    nodes.map(_ -> Nil).toMap[A, Seq[A]] ++ dependencies.groupBy(_._1).mapValues(_.map(_._2))

  private lazy val duplicatedIds =
    nodes.groupBy(_.id).filter(_._2.toSet.size > 1).keySet

  final def toActivities: Iterable[A] = {
    assert(duplicatedIds.isEmpty, s"Duplicated ids detected: ${duplicatedIds.mkString(", ")}")

    var unresolved = dependencyGraph
    var resolved = Map.empty[A,A]

    while(!unresolved.isEmpty){
      val nodes = unresolved.filter(_._2.isEmpty).keySet
      assert(!nodes.isEmpty, "Cyclic dependencies detected")

      resolved ++= nodes.map(n => n -> n.dependsOn(dependencyGraph(n).map(resolved).sortBy(_.id):_*))
      unresolved = unresolved.filterKeys(!nodes.contains(_)).mapValues(_.filterNot(nodes))
    }
    resolved.values.toSeq
  }
}

object WorkflowGraph{

  private def apply(
    nodes: Seq[PipelineActivity[_ <: ResourceObject]],
    roots: Seq[PipelineActivity[_ <: ResourceObject]],
    leaves: Seq[PipelineActivity[_ <: ResourceObject]],
    dependencies: Seq[(PipelineActivity[_ <: ResourceObject], PipelineActivity[_ <: ResourceObject])]
  ): WorkflowGraph = {
    new WorkflowGraph(nodes.distinct, roots.distinct, leaves.distinct, dependencies.distinct)
  }

  def apply(): WorkflowGraph = new WorkflowGraph(Nil, Nil, Nil, Nil)

  def apply(act: PipelineActivity[_ <: ResourceObject]): WorkflowGraph = new WorkflowGraph(Seq(act), Seq(act), Seq(act), Nil)

  def apply(exp: WorkflowExpression): WorkflowGraph = exp match {
    case WorkflowNoActivityExpression => WorkflowGraph()
    case WorkflowActivityExpression(act) => WorkflowGraph(act)
    case WorkflowPlusExpression(left, right) => WorkflowGraph(left) ++ WorkflowGraph(right)
    case WorkflowArrowExpression(left, right) => WorkflowGraph(left) ~> WorkflowGraph(right)
  }
}
