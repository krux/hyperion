package com.krux.hyperion.h3.activity

import com.krux.hyperion.adt.HString
import com.krux.hyperion.resource.EmrCluster

/**
 * The base trait for activities that run on an Amazon EMR cluster
 */
trait EmrActivity[A <: EmrCluster] extends PipelineActivity[A] {

  type Self <: EmrActivity[A]

  def emrActivityFields: EmrActivityFields
  def updateEmrActivityFields(fields: EmrActivityFields): Self

  def preStepCommands = emrActivityFields.preStepCommands
  def withPreStepCommand(commands: HString*): Self = updateEmrActivityFields(
    emrActivityFields.copy(preStepCommands = emrActivityFields.preStepCommands ++ commands)
  )

  def postStepCommands = emrActivityFields.postStepCommands
  def withPostStepCommand(commands: HString*): Self = updateEmrActivityFields(
    emrActivityFields.copy(postStepCommands = emrActivityFields.postStepCommands ++ commands)
  )

}
