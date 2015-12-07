package com.krux.hyperion.h3.activity

import shapeless._

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.activity.Script
import com.krux.hyperion.adt.{HInt, HDuration, HString, HBoolean}
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.aws.{ AdpActivity, AdpRef }
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.h3.common.{ PipelineObject, ObjectFields }
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{ Resource, Ec2Resource }

trait BaseShellCommandActivity extends PipelineActivity[Ec2Resource] {

  type Self <: BaseShellCommandActivity

  def shellCommandActivityFields: ShellCommandActivityFields
  def shellCommandActivityFieldsLens: Lens[Self, ShellCommandActivityFields]

  def script = shellCommandActivityFields.script

  def scriptArguments = shellCommandActivityFields.scriptArguments
  def withArguments(args: HString*): Self =
    (shellCommandActivityFieldsLens >> 'scriptArguments).modify(self)(_ ++ args)

  def stdout = shellCommandActivityFields.stdout
  def withStdoutTo(out: HString): Self =
    (shellCommandActivityFieldsLens >> 'stdout).set(self)(Option(out))

  def stderr = shellCommandActivityFields.stderr
  def withStderrTo(err: HString): Self =
    (shellCommandActivityFieldsLens >> 'stderr).set(self)(Option(err))

  def stage = shellCommandActivityFields.stage

  def input = shellCommandActivityFields.input
  def withInput(inputs: S3DataNode*): Self = {
    (shellCommandActivityFieldsLens >> 'input).modify(self)(_ ++ inputs)
    (shellCommandActivityFieldsLens >> 'stage).set(self)(Option(HBoolean.True))
  }

  def output = shellCommandActivityFields.output
  def withOutput(outputs: S3DataNode*): Self = {
    (shellCommandActivityFieldsLens >> 'output).modify(self)(_ ++ outputs)
    (shellCommandActivityFieldsLens >> 'stage).set(self)(Option(HBoolean.True))
  }

  // TODO: Uncomment the following once they are transformed
  override def objects = super.objects // :+ input :+ output

  def serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = script.content.map(_.serialize),
    scriptUri = script.uri.map(_.serialize),
    scriptArgument = scriptArguments.map(_.serialize),
    stdout = stdout.map(_.serialize),
    stderr = stderr.map(_.serialize),
    stage = stage.map(_.serialize),
    input = seqToOption(input)(_.ref),
    output = seqToOption(output)(_.ref),
    workerGroup = runsOn.asWorkerGroup.map(_.ref),
    runsOn = runsOn.asManagedResource.map(_.ref),
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref),
    attemptTimeout = attemptTimeout.map(_.serialize),
    lateAfterTimeout = lateAfterTimeout.map(_.serialize),
    maximumRetries = maximumRetries.map(_.serialize),
    retryDelay = retryDelay.map(_.serialize),
    failureAndRerunMode = failureAndRerunMode.map(_.serialize)
  )
}
