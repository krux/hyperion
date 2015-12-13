package com.krux.hyperion.activity

import com.krux.hyperion.adt.{ HString, HBoolean }
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.resource.{ Resource, Ec2Resource }

trait GoogleStorageActivity extends PipelineActivity[Ec2Resource] {

  type Self <: GoogleStorageActivity

  def shellCommandActivityFields: ShellCommandActivityFields
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields): Self

  def script = shellCommandActivityFields.script

  def scriptArguments = shellCommandActivityFields.scriptArguments
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

  private[hyperion] def serializedInput: Seq[S3DataNode] = Seq.empty

  private[hyperion] def serializedOutput: Seq[S3DataNode] = Seq.empty

  override def objects = serializedInput ++ serializedOutput ++ super.objects

  def serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = script.content.map(_.serialize),
    scriptUri = script.uri.map(_.serialize),
    scriptArgument = scriptArguments.map(_.serialize),
    stdout = stdout.map(_.serialize),
    stderr = stderr.map(_.serialize),
    stage = stage.map(_.serialize),
    input = seqToOption(serializedInput)(_.ref),
    output = seqToOption(serializedOutput)(_.ref),
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
