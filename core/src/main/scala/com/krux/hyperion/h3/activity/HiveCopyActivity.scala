package com.krux.hyperion.h3.activity

import com.krux.hyperion.adt.{ HString, HS3Uri }
import com.krux.hyperion.aws.AdpHiveCopyActivity
import com.krux.hyperion.h3.common.PipelineObjectId
import com.krux.hyperion.h3.datanode.DataNode
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.h3.common.ObjectFields
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.h3.resource.{ Resource, EmrCluster }

/**
 * Runs a Hive query on an Amazon EMR cluster. HiveCopyActivity makes it easier to copy data between
 * Amazon S3 and DynamoDB. HiveCopyActivity accepts a HiveQL statement to filter input data from
 * Amazon S3 or DynomoDB at the column and row level.
 */
case class HiveCopyActivity[A <: EmrCluster] private (
  baseFields: ObjectFields,
  activityFields: ActivityFields[A],
  emrTaskActivityFields: EmrTaskActivityFields,
  filterSql: Option[HString],
  generatedScriptsPath: Option[HS3Uri],
  input: DataNode,
  output: DataNode
) extends EmrTaskActivity[A] {

  type Self = HiveCopyActivity[A]

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[A]) = copy(activityFields = fields)
  def updateEmrTaskActivityFields(fields: EmrTaskActivityFields) = copy(emrTaskActivityFields = fields)

  def withFilterSql(filterSql: HString) = this.copy(filterSql = Option(filterSql))
  def withGeneratedScriptsPath(generatedScriptsPath: HS3Uri) = this.copy(generatedScriptsPath = Option(generatedScriptsPath))

  // def objects: Iterable[PipelineObject] = runsOn.toSeq ++ Seq(input, output) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms ++ preActivityTaskConfig.toSeq ++ postActivityTaskConfig.toSeq

  lazy val serialize = AdpHiveCopyActivity(
    id = id,
    name = id.toOption,
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
    failureAndRerunMode = failureAndRerunMode.map(_.serialize)
  )
}

object HiveCopyActivity extends RunnableObject {

  def apply[A <: EmrCluster](input: DataNode, output: DataNode)(runsOn: Resource[A]): HiveCopyActivity[A] =
    new HiveCopyActivity(
      baseFields = ObjectFields(PipelineObjectId(HiveActivity.getClass)),
      activityFields = ActivityFields(runsOn),
    emrTaskActivityFields = EmrTaskActivityFields(),
      filterSql = None,
      generatedScriptsPath = None,
      input = input,
      output = output
    )

}
