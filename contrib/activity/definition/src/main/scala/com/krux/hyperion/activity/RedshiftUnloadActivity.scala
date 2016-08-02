package com.krux.hyperion.activity

import scala.annotation.tailrec
import scala.collection.mutable.StringBuilder

import com.krux.hyperion.adt.{HS3Uri, HString}
import com.krux.hyperion.aws.AdpSqlActivity
import com.krux.hyperion.common.{BaseFields, PipelineObjectId, Escapable}
import com.krux.hyperion.database.RedshiftDatabase
import com.krux.hyperion.expression.{EncryptedParameter, RunnableObject}
import com.krux.hyperion.resource.{Ec2Resource, Resource}

/**
 * Unload result of the given sql script from redshift to given s3Path.
 */
case class RedshiftUnloadActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  script: HString,
  s3Path: HS3Uri,
  database: RedshiftDatabase,
  unloadOptions: Seq[RedshiftUnloadOption],
  queue: Option[HString],
  accessKeyId: EncryptedParameter[String],
  accessKeySecret: EncryptedParameter[String]
) extends PipelineActivity[Ec2Resource] {

  type Self = RedshiftUnloadActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)

  def unloadScript = s"""
    |UNLOAD ('${Escapable.escape(script.serialize, '\'')}')
    |TO '$s3Path'
    |WITH CREDENTIALS AS
    |'aws_access_key_id=${accessKeyId.ref};aws_secret_access_key=${accessKeySecret.ref}'
    |${unloadOptions.flatMap(_.repr).mkString(" ")}
  """.stripMargin

  def withUnloadOptions(opts: RedshiftUnloadOption*) = copy(unloadOptions = unloadOptions ++ opts)

  def withQueue(queue: HString) = copy(queue = Option(queue))

  override def objects = Seq(database) ++ super.objects

  lazy val serialize = AdpSqlActivity(
    id = id,
    name = name,
    script = Option(unloadScript),
    scriptUri = None,
    scriptArgument = None,
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
    failureAndRerunMode = failureAndRerunMode.map(_.serialize),
    maxActiveInstances = maxActiveInstances.map(_.serialize)
  )

}

object RedshiftUnloadActivity extends RunnableObject {

  def apply(database: RedshiftDatabase, script: HString, s3Path: HS3Uri,
    accessKeyId: EncryptedParameter[String], accessKeySecret: EncryptedParameter[String])(runsOn: Resource[Ec2Resource]): RedshiftUnloadActivity =
    new RedshiftUnloadActivity(
      baseFields = BaseFields(PipelineObjectId(RedshiftUnloadActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      script = script,
      s3Path = s3Path,
      database = database,
      queue = None,
      unloadOptions = Seq.empty,
      accessKeyId = accessKeyId,
      accessKeySecret = accessKeySecret
    )

}
