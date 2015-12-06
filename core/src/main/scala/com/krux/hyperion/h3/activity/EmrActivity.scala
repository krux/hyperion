package com.krux.hyperion.h3.activity

import shapeless._

import com.krux.hyperion.adt.HString
import com.krux.hyperion.resource.EmrCluster

/**
 * The base trait for activities that run on an Amazon EMR cluster
 */
trait EmrActivity[A <: EmrCluster] extends PipelineActivity[A] {

  type Self <: EmrActivity[A]

  def emrActivityFields: EmrActivityFields
  def emrActivityFieldsLens: Lens[Self, EmrActivityFields]

  def preStepCommands = emrActivityFieldsLens.preStepCommands
  def withPreStepCommand(commands: HString*): Self =
    (emrActivityFieldsLens >> 'preStepCommands).modify(self)(_ ++ commands)

  def postStepCommands = emrActivityFieldsLens.postStepCommands
  def withPostStepCommand(commands: HString*): Self =
    (emrActivityFieldsLens >> 'postStepCommands).modify(self)(_ ++ commands)

}
