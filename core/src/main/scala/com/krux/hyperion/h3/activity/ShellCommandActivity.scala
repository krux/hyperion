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

/**
 * Runs a command or script
 */
case class ShellCommandActivity private (
  baseFields: ObjectFields,
  activityFields: ActivityFields[Ec2Resource],
  script: Script,
  scriptArguments: Seq[HString],
  stdout: Option[HString],
  stderr: Option[HString],
  stage: Option[HBoolean],
  input: Seq[S3DataNode],
  output: Seq[S3DataNode]
) extends PipelineActivity[Ec2Resource] {

  type Self = ShellCommandActivity

  def baseFieldsLens = lens[Self] >> 'baseFields
  def activityFieldsLens = lens[Self] >> 'activityFields

  def withArguments(args: HString*) = this.copy(scriptArguments = scriptArguments ++ args)
  def withStdoutTo(out: HString) = this.copy(stdout = Option(out))
  def withStderrTo(err: HString) = this.copy(stderr = Option(err))
  def withInput(inputs: S3DataNode*) = this.copy(input = input ++ inputs, stage = Option(HBoolean.True))
  def withOutput(outputs: S3DataNode*) = this.copy(output = output ++ outputs, stage = Option(HBoolean.True))

  // TODO: Uncomment the following once they are transformed
  override def objects = super.objects // :+ input :+ output

  lazy val serialize = AdpShellCommandActivity(
    id = baseFields.id,
    name = baseFields.id.toOption,
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
    dependsOn = seqToOption(activityFields.dependsOn)(_.ref),
    precondition = seqToOption(activityFields.preconditions)(_.ref),
    onFail = seqToOption(activityFields.onFailAlarms)(_.ref),
    onSuccess = seqToOption(activityFields.onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(activityFields.onLateActionAlarms)(_.ref),
    attemptTimeout = activityFields.attemptTimeout.map(_.serialize),
    lateAfterTimeout = activityFields.lateAfterTimeout.map(_.serialize),
    maximumRetries = activityFields.maximumRetries.map(_.serialize),
    retryDelay = activityFields.retryDelay.map(_.serialize),
    failureAndRerunMode = activityFields.failureAndRerunMode.map(_.serialize)
  )
}

object ShellCommandActivity extends RunnableObject {

  def apply(script: Script)(runsOn: Resource[Ec2Resource]): ShellCommandActivity =
    new ShellCommandActivity(
      baseFields = ObjectFields(PipelineObjectId(ShellCommandActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      script = script,
      scriptArguments = Seq.empty,
      stdout = None,
      stderr = None,
      stage = None,
      input = Seq.empty,
      output = Seq.empty
    )

}
