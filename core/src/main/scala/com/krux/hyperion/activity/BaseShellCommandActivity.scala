package com.krux.hyperion.activity

import com.krux.hyperion.adt.{ HType, HString }
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.resource.Ec2Resource

trait BaseShellCommandActivity extends PipelineActivity[Ec2Resource] {

  type Self <: BaseShellCommandActivity

  def shellCommandActivityFields: ShellCommandActivityFields
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields): Self

  def script = shellCommandActivityFields.script

  def scriptArguments: Seq[HType] = shellCommandActivityFields.scriptArguments
  def withArguments(args: HString*): Self = updateShellCommandActivityFields(
    shellCommandActivityFields.copy(scriptArguments = shellCommandActivityFields.scriptArguments ++ args)
  )

  def stdout = shellCommandActivityFields.stdout
  def withStdoutTo(out: HString): Self = updateShellCommandActivityFields(
    shellCommandActivityFields.copy(stdout = Option(out))
  )

  def stderr = shellCommandActivityFields.stderr
  def withStderrTo(err: HString): Self = updateShellCommandActivityFields(
    shellCommandActivityFields.copy(stderr = Option(err))
  )

  def stage = shellCommandActivityFields.stage

  def input = shellCommandActivityFields.input

  def output = shellCommandActivityFields.output

  override def objects = input ++ output ++ super.objects

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
