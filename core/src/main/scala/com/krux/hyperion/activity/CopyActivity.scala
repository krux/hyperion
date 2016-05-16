package com.krux.hyperion.activity

import com.krux.hyperion.aws.AdpCopyActivity
import com.krux.hyperion.common.{ PipelineObjectId, BaseFields }
import com.krux.hyperion.datanode.Copyable
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.resource.{ Resource, Ec2Resource }

/**
 * The activity that copies data from one data node to the other.
 *
 * @note it seems that both input and output format needs to be in CsvDataFormat for this copy to
 * work properly and it needs to be a specific variance of the CSV, for more information check the
 * web page:
 *
 * http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-copyactivity.html
 *
 * From our experience it's really hard to export using TsvDataFormat, in both import and export
 * especially for tasks involving RedshiftCopyActivity. A general rule of thumb is always use
 * default CsvDataFormat for tasks involving both exporting to S3 and copy to redshift.
 */
case class CopyActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  input: Copyable,
  output: Copyable
) extends PipelineActivity[Ec2Resource] {

  type Self = CopyActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)

  override def objects = Seq(input, output) ++ super.objects

  lazy val serialize = AdpCopyActivity(
    id = id,
    name = name,
    input = input.ref,
    output = output.ref,
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

object CopyActivity extends RunnableObject {

  def apply(input: Copyable, output: Copyable)(runsOn: Resource[Ec2Resource]): CopyActivity =
    new CopyActivity(
      baseFields = BaseFields(PipelineObjectId(CopyActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      input = input,
      output = output
    )

}
