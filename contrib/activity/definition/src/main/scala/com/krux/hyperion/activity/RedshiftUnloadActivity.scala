package com.krux.hyperion.activity

import scala.annotation.tailrec
import scala.collection.mutable.StringBuilder

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpSqlActivity
import com.krux.hyperion.common.{ PipelineObjectId, PipelineObject, BaseFields }
import com.krux.hyperion.database.RedshiftDatabase
import com.krux.hyperion.expression.{ RunnableObject, Parameter }
import com.krux.hyperion.adt.{ HInt, HDuration, HS3Uri, HString }
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{ Resource, Ec2Resource }

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
  accessKeyId: Parameter[String],
  accessKeySecret: Parameter[String]
) extends PipelineActivity[Ec2Resource] {

  require(accessKeyId.isEncrypted, "The access key id must be an encrypted string parameter")
  require(accessKeySecret.isEncrypted, "The access secret must be an encrypted string parameter")

  type Self = RedshiftUnloadActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)

  /**
   * Given the start of exp, seek the end of expression returning the expression block and the rest
   * of the string. Note that expression is not a nested structure and the only legitimate '{' or
   * '}' within a expression is within quotes (i.e. '"' or "'")
   *
   * @note this does not handle the case that expression have escaped quotes (i.e. "\"" or '\'')
   */
  @tailrec
  private def seekEndOfExpr(
      exp: String,
      quote: Option[Char] = None,
      expPart: StringBuilder = StringBuilder.newBuilder
    ): (String, String) = {

    if (exp.isEmpty) {
      throw new RuntimeException("expression started but not ended")
    } else {
      val curChar = exp.head
      val next = exp.tail

      quote match {
        case Some(quoteChar) =>  // if is in quote
          seekEndOfExpr(next, quote.filter(_ != curChar), expPart += curChar)
        case _ =>
          curChar match {
            case '}' => ((expPart += curChar).result, next)
            case '\'' | '"' => seekEndOfExpr(next, Option(curChar), expPart += curChar)
            case _ => seekEndOfExpr(next, None, expPart += curChar)
          }
      }
    }
  }

  private def escapeChar(c: Char): String = if (c == '\'') "\\\\'" else c.toString

  @tailrec
  private def prepareScript(
      exp: String,
      hashSpotted: Boolean = false,
      result: StringBuilder = StringBuilder.newBuilder
    ): String = {

    if (exp.isEmpty) {
      result.toString
    } else {
      val curChar = exp.head
      val expTail = exp.tail

      if (!hashSpotted) {  // outside a expression block
        prepareScript(expTail, curChar == '#', result ++= escapeChar(curChar))
      } else {  // the previous char is '#'
        if (curChar == '{') {  // start of an expression
          val (blockBody, rest) = seekEndOfExpr(expTail)
          prepareScript(rest, false, result += curChar ++= blockBody)
        } else {  // not start of an expression
          prepareScript(expTail, false, result ++= escapeChar(curChar))
        }
      }
    }
  }

  def unloadScript = s"""
    |UNLOAD ('${prepareScript(script.serialize)}')
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
    name = id.toOption,
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
    failureAndRerunMode = failureAndRerunMode.map(_.serialize)
  )

}

object RedshiftUnloadActivity extends RunnableObject {

  def apply(database: RedshiftDatabase, script: HString, s3Path: HS3Uri,
    accessKeyId: Parameter[String], accessKeySecret: Parameter[String])(runsOn: Resource[Ec2Resource]): RedshiftUnloadActivity =
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
