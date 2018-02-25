package com.krux.hyperion.activity

import com.krux.hyperion.adt.{HString, HBoolean}
import com.krux.hyperion.aws.AdpHiveActivity
import com.krux.hyperion.common.BaseFields
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.datanode.DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.resource.{Resource, BaseEmrCluster}

/**
 * Runs a Hive query on an Amazon EMR cluster. HiveActivity makes it easier to set up an Amzon EMR
 * activity and automatically creates Hive tables based on input data coming in from either Amazon
 * S3 or Amazon RDS. All you need to specify is the HiveQL to run on the source data. AWS Data
 * Pipeline automatically creates Hive tables with \${input1}, \${input2}, etc. based on the input
 * fields in the Hive Activity object. For S3 inputs, the dataFormat field is used to create the
 * Hive column names. For MySQL (RDS) inputs, the column names for the SQL query are used to create
 * the Hive column names.
 */
case class HiveActivity[A <: BaseEmrCluster] private (
  baseFields: BaseFields,
  activityFields: ActivityFields[A],
  emrTaskActivityFields: EmrTaskActivityFields,
  hiveScript: Script,
  scriptVariables: Seq[HString],
  input: Seq[DataNode],
  output: Seq[DataNode],
  hadoopQueue: Option[HString]
) extends EmrTaskActivity[A] {

  type Self = HiveActivity[A]

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[A]) = copy(activityFields = fields)
  def updateEmrTaskActivityFields(fields: EmrTaskActivityFields) = copy(emrTaskActivityFields = fields)

  def withScriptVariable(scriptVariable: HString*) = copy(scriptVariables = scriptVariables ++ scriptVariable)
  def withHadoopQueue(queue: HString) = copy(hadoopQueue = Option(queue))

  override def objects = input ++ output ++ super.objects

  lazy val serialize = new AdpHiveActivity(
    id = id,
    name = name,
    hiveScript = hiveScript.content.map(_.serialize),
    scriptUri = hiveScript.uri.map(_.serialize),
    scriptVariable = seqToOption(scriptVariables)(_.serialize),
    stage = Option(HBoolean.True.serialize),
    input = seqToOption(input)(_.ref),
    output = seqToOption(output)(_.ref),
    hadoopQueue = hadoopQueue.map(_.serialize),
    preActivityTaskConfig = preActivityTaskConfig.map(_.ref),
    postActivityTaskConfig = postActivityTaskConfig.map(_.ref),
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

object HiveActivity extends RunnableObject {

  def apply[A <: BaseEmrCluster](input: Seq[DataNode], output: Seq[DataNode], hiveScript: Script)(runsOn: Resource[A]): HiveActivity[A] =
    new HiveActivity(
      baseFields = BaseFields(PipelineObjectId(HiveActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      emrTaskActivityFields = EmrTaskActivityFields(),
      hiveScript = hiveScript,
      scriptVariables = Seq.empty,
      input = input,
      output = output,
      hadoopQueue = None
    )

  def apply[A <: BaseEmrCluster](input: DataNode, output: DataNode, hiveScript: Script)(runsOn: Resource[A]): HiveActivity[A] =
    apply(Seq(input), Seq(output), hiveScript)(runsOn)

}
