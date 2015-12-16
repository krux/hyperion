package com.krux.hyperion.activity

import com.krux.hyperion.adt.HString
import com.krux.hyperion.aws.AdpSqlActivity
import com.krux.hyperion.database.Database
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.common.{ PipelineObjectId, BaseFields }
import com.krux.hyperion.resource.{ Resource, Ec2Resource }

/**
 * Runs an SQL query on a RedShift cluster. If the query writes out to a table that does not exist,
 * a new table with that name is created.
 */
case class SqlActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  script: Script,
  scriptArgument: Seq[HString],
  database: Database,
  queue: Option[HString]
) extends PipelineActivity[Ec2Resource] {

  type Self = SqlActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)

  def withArguments(arg: HString*) = copy(scriptArgument = scriptArgument ++ arg)
  def withQueue(queue: HString) = copy(queue = Option(queue))

  override def objects = Seq(database) ++ super.objects

  lazy val serialize = AdpSqlActivity(
    id = id,
    name = id.toOption,
    script = script.content.map(_.serialize),
    scriptUri = script.uri.map(_.serialize),
    scriptArgument = scriptArgument.map(_.serialize),
    database = database.ref,
    queue = queue.map(_.serialize),
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

object SqlActivity extends RunnableObject {

  def apply(database: Database, script: Script)(runsOn: Resource[Ec2Resource]): SqlActivity =
    new SqlActivity(
      baseFields = BaseFields(PipelineObjectId(SqlActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      script = script,
      scriptArgument = Seq.empty,
      database = database,
      queue = None
    )

}
