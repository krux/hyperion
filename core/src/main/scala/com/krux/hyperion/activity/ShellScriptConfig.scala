package com.krux.hyperion.activity

import com.krux.hyperion.aws.{ AdpRef, AdpShellScriptConfig }
import com.krux.hyperion.common.{ PipelineObject, BaseFields, PipelineObjectId, NamedPipelineObject }
import com.krux.hyperion.adt.{ HS3Uri, HString }

case class ShellScriptConfig(
  baseFields: BaseFields,
  scriptUri: HS3Uri,
  scriptArguments: Seq[HString]
) extends NamedPipelineObject {

  type Self = ShellScriptConfig

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)

  def withArguments(args: HString*) = copy(scriptArguments = scriptArguments ++ args)

  def objects: Iterable[PipelineObject] = None

  lazy val serialize = AdpShellScriptConfig(
    id = id,
    name = id.toOption,
    scriptUri = scriptUri.serialize,
    scriptArgument = scriptArguments.map(_.serialize)
  )

  def ref: AdpRef[AdpShellScriptConfig] = AdpRef(serialize)
}

object ShellScriptConfig {

  def apply(scriptUri: HS3Uri): ShellScriptConfig =
    new ShellScriptConfig(
      baseFields = BaseFields(PipelineObjectId(ShellScriptConfig.getClass)),
      scriptUri = scriptUri,
      scriptArguments = Seq.empty
    )

}

