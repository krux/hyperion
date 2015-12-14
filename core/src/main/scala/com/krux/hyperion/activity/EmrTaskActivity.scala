package com.krux.hyperion.activity

import com.krux.hyperion.resource.EmrCluster

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

  override def objects = preActivityTaskConfig ++: postActivityTaskConfig ++: super.objects

}
