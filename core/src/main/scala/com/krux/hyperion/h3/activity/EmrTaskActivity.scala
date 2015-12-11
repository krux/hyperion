package com.krux.hyperion.h3.activity

import com.krux.hyperion.h3.resource.EmrCluster

trait EmrTaskActivity[A <: EmrCluster] extends EmrActivity[A] {

  type Self <: EmrTaskActivity[A]

  def emrTaskActivityFields: EmrTaskActivityFields
  def updateEmrTaskActivityFields(fields: EmrTaskActivityFields): Self

  def preActivityTaskConfig = emrTaskActivityFields.preActivityTaskConfig
  def withPreActivityTaskConfig(config: ShellScriptConfig): Self = updateEmrTaskActivityFields(
    emrTaskActivityFields.copy(preActivityTaskConfig = Option(config))
  )

  def postActivityTaskConfig = emrTaskActivityFields.postActivityTaskConfig
  def withPostActivityTaskConfig(config: ShellScriptConfig): Self = updateEmrTaskActivityFields(
    emrTaskActivityFields.copy(postActivityTaskConfig = Option(config))
  )

}
