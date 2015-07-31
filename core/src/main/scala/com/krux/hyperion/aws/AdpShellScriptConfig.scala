package com.krux.hyperion.aws

case class AdpShellScriptConfig(
  id: String,
  name: Option[String],
  scriptUri: String,
  scriptArgument: Option[Seq[String]]
) extends AdpDataPipelineObject {

  val `type` = "ShellScriptConfig"

}
