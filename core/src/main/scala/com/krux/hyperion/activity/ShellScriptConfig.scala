package com.krux.hyperion.activity

import com.krux.hyperion.aws.{AdpRef, AdpShellScriptConfig}
import com.krux.hyperion.common.{S3Uri, PipelineObject, PipelineObjectId}
import com.krux.hyperion.parameter.Parameter

case class ShellScriptConfig(
  id: PipelineObjectId,
  scriptUri: Parameter[S3Uri],
  scriptArguments: Seq[String]
) extends PipelineObject {

  def withArguments(args: String*) = this.copy(scriptArguments = scriptArguments ++ args)

  def objects: Iterable[PipelineObject] = None

  lazy val serialize = AdpShellScriptConfig(
    id = id,
    name = id.toOption,
    scriptUri = scriptUri.toString,
    scriptArgument = scriptArguments match {
      case Seq() => None
      case args => Option(args)
    }
  )

  def ref: AdpRef[AdpShellScriptConfig] = AdpRef(serialize)
}

object ShellScriptConfig {
  def apply(scriptUri: Parameter[S3Uri]): ShellScriptConfig =
    new ShellScriptConfig(
      id = PipelineObjectId(ShellScriptConfig.getClass),
      scriptUri = scriptUri,
      scriptArguments = Seq()
    )
}

