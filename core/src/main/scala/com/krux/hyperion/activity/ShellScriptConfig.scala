package com.krux.hyperion.activity

import com.krux.hyperion.aws.{AdpRef, AdpShellScriptConfig}
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId}

case class ShellScriptConfig(
  id: PipelineObjectId,
  scriptUri: String,
  scriptArguments: Seq[String]
) extends PipelineObject {

  def withArguments(args: String*) = this.copy(scriptArguments = scriptArguments ++ args)

  lazy val serialize = AdpShellScriptConfig(
    id = id,
    name = id.toOption,
    scriptUri = scriptUri,
    scriptArgument = scriptArguments match {
      case Seq() => None
      case args => Option(args)
    }
  )

  def ref: AdpRef[AdpShellScriptConfig] = AdpRef(serialize)
}

object ShellScriptConfig {
  def apply(scriptUri: String): ShellScriptConfig =
    new ShellScriptConfig(
      id = PipelineObjectId(ShellScriptConfig.getClass),
      scriptUri = scriptUri,
      scriptArguments = Seq()
    )
}

