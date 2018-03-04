package com.krux.hyperion.activity

import com.krux.hyperion.adt.{HString, HS3Uri}
import com.krux.hyperion.aws.AdpHiveCopyActivity
import com.krux.hyperion.common.BaseFields
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.datanode.DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.resource.{Resource, BaseEmrCluster}


/**
 * Runs a Hive query on an Amazon EMR cluster. HiveCopyActivity makes it easier to copy data between
 * Amazon S3 and DynamoDB. HiveCopyActivity accepts a HiveQL statement to filter input data from
 * Amazon S3 or DynomoDB at the column and row level.
 */
case class HiveCopyActivity[A <: BaseEmrCluster] private (
  baseFields: BaseFields,
  activityFields: ActivityFields[A],
  emrTaskActivityFields: EmrTaskActivityFields,
  filterSql: Option[HString],
  generatedScriptsPath: Option[HS3Uri],
  input: DataNode,
  output: DataNode
) extends EmrTaskActivity[A] {

  type Self = HiveCopyActivity[A]

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[A]) = copy(activityFields = fields)
  def updateEmrTaskActivityFields(fields: EmrTaskActivityFields) = copy(emrTaskActivityFields = fields)

  def withFilterSql(filterSql: HString) = copy(filterSql = Option(filterSql))
  def withGeneratedScriptsPath(generatedScriptsPath: HS3Uri) = copy(generatedScriptsPath = Option(generatedScriptsPath))

  override def objects = Seq(input, output) ++ super.objects

  lazy val serialize = AdpHiveCopyActivity(
    id = id,
    name = name,
    filterSql = filterSql.map(_.serialize),
    generatedScriptsPath = generatedScriptsPath.map(_.serialize),
    input = Option(input.ref),
    output = Option(output.ref),
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

object HiveCopyActivity extends RunnableObject {

  def apply[A <: BaseEmrCluster](input: DataNode, output: DataNode)(runsOn: Resource[A]): HiveCopyActivity[A] =
    new HiveCopyActivity(
      baseFields = BaseFields(PipelineObjectId(HiveCopyActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      emrTaskActivityFields = EmrTaskActivityFields(),
      filterSql = None,
      generatedScriptsPath = None,
      input = input,
      output = output
    )

}
