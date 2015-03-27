package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpEmrActivity, AdpJsonSerializer, AdpRef, AdpEmrCluster,
  AdpActivity}

/**
 * Defines a spark activity
 */
case class SparkActivity (
    id: String,
    runsOn: SparkCluster,
    steps: Seq[SparkStep] = Seq(),
    dependsOn: Seq[PipelineActivity] = Seq()
  ) extends EmrActivity {

  def withStepSeq(steps: Seq[SparkStep]) = this.copy(steps = steps)
  def withSteps(steps: SparkStep*) = this.copy(steps = steps)

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ dependsOn

  def serialize = AdpEmrActivity(
      id,
      Some(id),
      None,
      None,
      None,
      None,
      None,
      None,
      steps.map(_.toStepString),
      AdpRef[AdpEmrCluster](runsOn.id),
      dependsOn match {
        case Seq() => None
        case deps => Some(deps.map(d => AdpRef[AdpActivity](d.id)))
      }
    )
}

object SparkActivity extends RunnableObject
