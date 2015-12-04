package com.krux.hyperion.h3.activity

import shapeless._

import com.krux.hyperion.adt.HString
import com.krux.hyperion.resource.EmrCluster

/**
 * The base trait for activities that run on an Amazon EMR cluster
 */
trait EmrActivity[A <: EmrCluster, B <: EmrStep] extends PipelineActivity[A] {

  type Self <: EmrActivity[A, B]

  def emrActivityFields: EmrActivityFields[B]
  def emrActivityFieldsLens: Lens[Self, EmrActivityFields[B]]

  def withPreStepCommand(commands: HString*): Self =
    (emrActivityFieldsLens >> 'preStepCommands).modify(self)(_ ++ commands)

  def withPostStepCommand(commands: HString*): Self =
    (emrActivityFieldsLens >> 'postStepCommands).modify(self)(_ ++ commands)

  def withSteps(steps: B*): Self =
    (emrActivityFieldsLens >> 'steps).modify(self)(_ ++ steps)

}
