package com.krux.hyperion.activity

case class EmrTaskActivityFields(
  preActivityTaskConfig: Option[ShellScriptConfig] = None,
  postActivityTaskConfig: Option[ShellScriptConfig] = None
)
