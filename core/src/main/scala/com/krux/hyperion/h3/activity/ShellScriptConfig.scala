package com.krux.hyperion.h3.activity

import com.krux.hyperion.aws.{ AdpRef, AdpShellScriptConfig }
import com.krux.hyperion.h3.common.{ PipelineObject, ObjectFields, PipelineObjectId }
import com.krux.hyperion.adt.{ HS3Uri, HString }

case class ShellScriptConfig(
  baseFields: ObjectFields,
  scriptUri: HS3Uri,
  scriptArguments: Seq[HString]
) extends PipelineObject {

  type Self = ShellScriptConfig

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)

  def withArguments(args: HString*) = this.copy(scriptArguments = scriptArguments ++ args)

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
      baseFields = ObjectFields(PipelineObjectId(ShellScriptConfig.getClass)),
      scriptUri = scriptUri,
      scriptArguments = Seq.empty
    )

}

