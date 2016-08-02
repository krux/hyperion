package com.krux.hyperion.activity

import com.krux.hyperion.adt.{ HString, HS3Uri }
import com.krux.hyperion.aws.AdpHadoopActivity
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.common.{ BaseFields, PipelineObjectId }
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.resource.{ Resource, EmrCluster }

/**
 * Runs a MapReduce job on a cluster. The cluster can be an EMR cluster managed by AWS Data Pipeline
 * or another resource if you use TaskRunner. Use HadoopActivity when you want to run work in parallel.
 * This allows you to use the scheduling resources of the YARN framework or the MapReduce resource
 * negotiator in Hadoop 1. If you would like to run work sequentially using the Amazon EMR Step action,
 * you can still use EmrActivity.
 */
case class HadoopActivity[A <: EmrCluster] private (
  baseFields: BaseFields,
  activityFields: ActivityFields[A],
  emrTaskActivityFields: EmrTaskActivityFields,
  jarUri: HString,
  mainClass: Option[MainClass],
  arguments: Seq[HString],
  hadoopQueue: Option[HString],
  inputs: Seq[S3DataNode],
  outputs: Seq[S3DataNode]
) extends EmrTaskActivity[A] {

  type Self = HadoopActivity[A]

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[A]) = copy(activityFields = fields)
  def updateEmrTaskActivityFields(fields: EmrTaskActivityFields) = copy(emrTaskActivityFields = fields)

  def withArguments(arguments: HString*) = copy(arguments = arguments ++ arguments)
  def withHadoopQueue(queue: HString) = copy(hadoopQueue = Option(queue))
  def withInput(input: S3DataNode*) = copy(inputs = inputs ++ input)
  def withOutput(output: S3DataNode*) = copy(outputs = outputs ++ output)

  override def objects = inputs ++ outputs ++ super.objects

  lazy val serialize = new AdpHadoopActivity(
    id = id,
    name = name,
    jarUri = jarUri.serialize,
    mainClass = mainClass.map(_.toString),
    argument = arguments.map(_.serialize),
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
    failureAndRerunMode = failureAndRerunMode.map(_.serialize),
    maxActiveInstances = maxActiveInstances.map(_.serialize)
  )

}

object HadoopActivity extends RunnableObject {

  def apply[A <: EmrCluster](jarUri: HS3Uri)(runsOn: Resource[A]): HadoopActivity[A] = apply(jarUri, None)(runsOn)

  def apply[A <: EmrCluster](jarUri: HS3Uri, mainClass: MainClass)(runsOn: Resource[A]): HadoopActivity[A] = apply(jarUri, Option(mainClass))(runsOn)

  def apply[A <: EmrCluster](jarUri: HS3Uri, mainClass: Option[MainClass])(runsOn: Resource[A]): HadoopActivity[A] = apply(jarUri.serialize, mainClass)(runsOn)

  def apply[A <: EmrCluster](jarUri: HString, mainClass: MainClass)(runsOn: Resource[A]): HadoopActivity[A] = apply(jarUri, Option(mainClass))(runsOn)

  def apply[A <: EmrCluster](jarUri: HString, mainClass: Option[MainClass] = None)(runsOn: Resource[A]): HadoopActivity[A] = new HadoopActivity(
    baseFields = BaseFields(PipelineObjectId(HadoopActivity.getClass)),
    activityFields = ActivityFields(runsOn),
    emrTaskActivityFields = EmrTaskActivityFields(),
    jarUri = jarUri,
    mainClass = mainClass,
    arguments = Seq.empty,
    hadoopQueue = None,
    inputs = Seq.empty,
    outputs = Seq.empty
  )

}
