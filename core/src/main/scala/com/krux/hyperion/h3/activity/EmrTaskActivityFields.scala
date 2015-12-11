package com.krux.hyperion.h3.activity

case class EmrTaskActivityFields(
  preActivityTaskConfig: Option[ShellScriptConfig] = None,
  postActivityTaskConfig: Option[ShellScriptConfig] = None
)
