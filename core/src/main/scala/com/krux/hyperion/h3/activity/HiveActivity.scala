package com.krux.hyperion.h3.activity

import shapeless._

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.{ HInt, HDuration, HString, HBoolean }
import com.krux.hyperion.aws.AdpHiveActivity
import com.krux.hyperion.h3.common.PipelineObjectId
import com.krux.hyperion.datanode.DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.h3.common.ObjectFields
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{ Resource, EmrCluster }
import com.krux.hyperion.activity.{ Script, ShellScriptConfig }

/**
 * Runs a Hive query on an Amazon EMR cluster. HiveActivity makes it easier to set up an Amzon EMR
 * activity and automatically creates Hive tables based on input data coming in from either Amazon
 * S3 or Amazon RDS. All you need to specify is the HiveQL to run on the source data. AWS Data
 * Pipeline automatically creates Hive tables with \${input1}, \${input2}, etc. based on the input
 * fields in the Hive Activity object. For S3 inputs, the dataFormat field is used to create the
 * Hive column names. For MySQL (RDS) inputs, the column names for the SQL query are used to create
 * the Hive column names.
 */
case class HiveActivity[A <: EmrCluster] private (
  baseFields: ObjectFields,
  activityFields: ActivityFields[A],
  emrActivityFields: EmrActivityFields,
  hiveScript: Script,
  scriptVariables: Seq[HString],
  input: DataNode,
  output: DataNode,
  hadoopQueue: Option[HString],
  preActivityTaskConfig: Option[ShellScriptConfig],
  postActivityTaskConfig: Option[ShellScriptConfig]
) extends EmrActivity[A] {

  type Self = HiveActivity[A]

  def baseFieldsLens = lens[Self] >> 'baseFields
  def activityFieldsLens = lens[Self] >> 'activityFields
  def emrActivityFieldsLens = lens[Self] >> 'emrActivityFields

  def withScriptVariable(scriptVariable: HString*) = this.copy(scriptVariables = scriptVariables ++ scriptVariable)
  def withHadoopQueue(queue: HString) = this.copy(hadoopQueue = Option(queue))
  def withPreActivityTaskConfig(script: ShellScriptConfig) = this.copy(preActivityTaskConfig = Option(script))
  def withPostActivityTaskConfig(script: ShellScriptConfig) = this.copy(postActivityTaskConfig = Option(script))

  // def objects: Iterable[PipelineObject] = runsOn.toSeq ++ Seq(input, output) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms ++ preActivityTaskConfig.toSeq ++ postActivityTaskConfig.toSeq

  lazy val serialize = new AdpHiveActivity(
    id = id,
    name = id.toOption,
    hiveScript = hiveScript.content.map(_.serialize),
    scriptUri = hiveScript.uri.map(_.serialize),
    scriptVariable = seqToOption(scriptVariables)(_.serialize),
    stage = Option(HBoolean.True.serialize),
    input = Option(input.ref),
    output = Option(output.ref),
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
    failureAndRerunMode = failureAndRerunMode.map(_.serialize)
  )
}

object HiveActivity extends RunnableObject {

  def apply[A <: EmrCluster](input: DataNode, output: DataNode, hiveScript: Script)(runsOn: Resource[A]): HiveActivity[A] =
    new HiveActivity(
      baseFields = ObjectFields(PipelineObjectId(HiveActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      emrActivityFields = EmrActivityFields(),
      hiveScript = hiveScript,
      scriptVariables = Seq.empty,
      input = input,
      output = output,
      hadoopQueue = None,
      preActivityTaskConfig = None,
      postActivityTaskConfig = None
    )

}
