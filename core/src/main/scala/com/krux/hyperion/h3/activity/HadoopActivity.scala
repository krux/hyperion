package com.krux.hyperion.h3.activity

import shapeless._

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.activity.MainClass
import com.krux.hyperion.adt.{HInt, HDuration, HString}
import com.krux.hyperion.aws._
import com.krux.hyperion.h3.common.PipelineObjectId
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.h3.common.ObjectFields
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, EmrCluster}
import com.krux.hyperion.activity.ShellScriptConfig

/**
 * Runs a MapReduce job on a cluster. The cluster can be an EMR cluster managed by AWS Data Pipeline
 * or another resource if you use TaskRunner. Use HadoopActivity when you want to run work in parallel.
 * This allows you to use the scheduling resources of the YARN framework or the MapReduce resource
 * negotiator in Hadoop 1. If you would like to run work sequentially using the Amazon EMR Step action,
 * you can still use EmrActivity.
 */
case class HadoopActivity[A <: EmrCluster] private (
  baseFields: ObjectFields,
  activityFields: ActivityFields[A],
  emrActivityFields: EmrActivityFields,
  jarUri: HString,
  mainClass: Option[MainClass],
  argument: Seq[HString],
  hadoopQueue: Option[HString],
  preActivityTaskConfig: Option[ShellScriptConfig],
  postActivityTaskConfig: Option[ShellScriptConfig],
  inputs: Seq[S3DataNode],
  outputs: Seq[S3DataNode]
) extends EmrActivity[A] {

  type Self = HadoopActivity[A]

  def baseFieldsLens = lens[Self] >> 'baseFields
  def activityFieldsLens = lens[Self] >> 'activityFields
  def emrActivityFieldsLens = lens[Self] >> 'emrActivityFields

  def withArguments(arguments: HString*) = this.copy(argument = argument ++ arguments)
  def withHadoopQueue(queue: HString) = this.copy(hadoopQueue = Option(queue))
  def withPreActivityTaskConfig(script: ShellScriptConfig) = this.copy(preActivityTaskConfig = Option(script))
  def withPostActivityTaskConfig(script: ShellScriptConfig) = this.copy(postActivityTaskConfig = Option(script))
  def withInput(input: S3DataNode*) = this.copy(inputs = inputs ++ input)
  def withOutput(output: S3DataNode*) = this.copy(outputs = outputs ++ output)

  // def objects: Iterable[PipelineObject] = inputs ++ outputs ++ runsOn.toSeq ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms ++ preActivityTaskConfig.toSeq ++ postActivityTaskConfig.toSeq

  lazy val serialize = AdpHadoopActivity(
    id = id,
    name = id.toOption,
    jarUri = jarUri.serialize,
    mainClass = mainClass.map(_.toString),
    argument = argument.map(_.serialize),
    hadoopQueue = hadoopQueue.map(_.serialize),
    preActivityTaskConfig = preActivityTaskConfig.map(_.ref),
    postActivityTaskConfig = postActivityTaskConfig.map(_.ref),
    input = seqToOption(inputs)(_.ref),
    output = seqToOption(outputs)(_.ref),
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

object HadoopActivity extends RunnableObject {

  def apply[A <: EmrCluster](jarUri: HString, mainClass: MainClass)(runsOn: Resource[A]): HadoopActivity[A] = apply(jarUri, Option(mainClass))(runsOn)

  def apply[A <: EmrCluster](jarUri: HString, mainClass: Option[MainClass] = None)(runsOn: Resource[A]): HadoopActivity[A] = new HadoopActivity(
    baseFields = ObjectFields(PipelineObjectId(HadoopActivity.getClass)),
    activityFields = ActivityFields(runsOn),
    emrActivityFields = EmrActivityFields(),
    jarUri = jarUri,
    mainClass = mainClass,
    argument = Seq.empty,
    hadoopQueue = None,
    preActivityTaskConfig = None,
    postActivityTaskConfig = None,
    inputs = Seq.empty,
    outputs = Seq.empty
  )

}
