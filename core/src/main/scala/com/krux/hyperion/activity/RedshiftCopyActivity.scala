package com.krux.hyperion.activity

import com.krux.hyperion.aws.AdpRedshiftCopyActivity
import com.krux.hyperion.common.{ PipelineObjectId, PipelineObject, BaseFields }
import com.krux.hyperion.dataformat.{ CsvDataFormat, TsvDataFormat }
import com.krux.hyperion.datanode.{ S3DataNode, RedshiftDataNode }
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.adt.HString
import com.krux.hyperion.resource.{ Resource, Ec2Resource }

/**
 * Copies data directly from DynamoDB or Amazon S3 to Amazon Redshift. You can load data into a new
 * table, or easily merge data into an existing table.
 */
case class RedshiftCopyActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  insertMode: RedshiftCopyActivity.InsertMode,
  transformSql: Option[HString],
  queue: Option[HString],
  commandOptions: Seq[RedshiftCopyOption],
  input: S3DataNode,
  output: RedshiftDataNode
) extends PipelineActivity[Ec2Resource] {

  type Self = RedshiftCopyActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)

  def withCommandOptions(opts: RedshiftCopyOption*) = {
    // The following assertion is a mirror of AWS Server runtime assertion.
    assert(
      input.dataFormat
        .forall {
          case f: CsvDataFormat => false
          case f: TsvDataFormat => false
          case _ => true
        }
      ,
      "CSV or TSV format cannot be used with commandOptions"
    )

    copy(commandOptions = commandOptions ++ opts)
  }

  def withTransformSql(sql: HString) = copy(transformSql = Option(sql))
  def withQueue(queue: HString) = copy(queue = Option(queue))

  override def objects: Iterable[PipelineObject] = Seq(input, output) ++ super.objects

  lazy val serialize = AdpRedshiftCopyActivity(
    id = id,
    name = name,
    insertMode = insertMode.toString,
    transformSql = transformSql.map(_.serialize),
    queue = queue.map(_.serialize),
    commandOptions = seqToOption(commandOptions)(_.repr).map(_.flatten),
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

object RedshiftCopyActivity extends Enumeration with RunnableObject {

  type InsertMode = Value
  val KeepExisting = Value("KEEP_EXISTING")
  val OverwriteExisting = Value("OVERWRITE_EXISTING")
  val Truncate = Value("TRUNCATE")

  def apply(input: S3DataNode, output: RedshiftDataNode, insertMode: InsertMode)(runsOn: Resource[Ec2Resource]): RedshiftCopyActivity =
    new RedshiftCopyActivity(
      baseFields = BaseFields(PipelineObjectId(RedshiftCopyActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      insertMode = insertMode,
      transformSql = None,
      queue = None,
      commandOptions = Seq.empty,
      input = input,
      output = output
    )

}
