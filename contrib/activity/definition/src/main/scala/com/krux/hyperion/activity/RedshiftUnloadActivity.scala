package com.krux.hyperion.activity

import scala.annotation.tailrec
import scala.collection.mutable.StringBuilder

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpSqlActivity
import com.krux.hyperion.common.{S3Uri, PipelineObjectId, PipelineObject}
import com.krux.hyperion.database.RedshiftDatabase
import com.krux.hyperion.expression.Duration
import com.krux.hyperion.parameter.{Parameter, StringParameter}
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

/**
 * Unload result of the given sql script from redshift to given s3Path.
 */
case class RedshiftUnloadActivity private (
  id: PipelineObjectId,
  script: String,
  s3Path: Parameter[S3Uri],
  database: RedshiftDatabase,
  unloadOptions: Seq[RedshiftUnloadOption],
  queue: Option[String],
  runsOn: Resource[Ec2Resource],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm],
  accessKeyId: StringParameter,
  accessKeySecret: StringParameter,
  attemptTimeout: Option[Parameter[Duration]],
  lateAfterTimeout: Option[Parameter[Duration]],
  maximumRetries: Option[Parameter[Int]],
  retryDelay: Option[Parameter[Duration]],
  failureAndRerunMode: Option[FailureAndRerunMode]
) extends PipelineActivity {

  require(accessKeyId.isEncrypted, "The access key id must be an encrypted string parameter")
  require(accessKeySecret.isEncrypted, "The access secret must be an encrypted string parameter")

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
    |UNLOAD ('${prepareScript(script)}')
    |TO '$s3Path'
    |WITH CREDENTIALS AS
    |'aws_access_key_id=$accessKeyId;aws_secret_access_key=$accessKeySecret'
    |${unloadOptions.flatMap(_.repr).mkString(" ")}
  """.stripMargin

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  def withUnloadOptions(opts: RedshiftUnloadOption*) =
    this.copy(unloadOptions = unloadOptions ++ opts)

  def withQueue(queue: String) = this.copy(queue = Option(queue))

  private[hyperion] def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)
  def withAttemptTimeout(timeout: Parameter[Duration]) = this.copy(attemptTimeout = Option(timeout))
  def withLateAfterTimeout(timeout: Parameter[Duration]) = this.copy(lateAfterTimeout = Option(timeout))
  def withMaximumRetries(retries: Parameter[Int]) = this.copy(maximumRetries = Option(retries))
  def withRetryDelay(delay: Parameter[Duration]) = this.copy(retryDelay = Option(delay))
  def withFailureAndRerunMode(mode: FailureAndRerunMode) = this.copy(failureAndRerunMode = Option(mode))

  def objects: Iterable[PipelineObject] =
    runsOn.toSeq ++
    Seq(database) ++
    dependsOn ++
    preconditions ++
    onFailAlarms ++
    onSuccessAlarms ++
    onLateActionAlarms

  lazy val serialize = AdpSqlActivity(
    id = id,
    name = id.toOption,
    script = Option(unloadScript),
    scriptUri = None,
    scriptArgument = None,
    database = database.ref,
    queue = queue,
    workerGroup = runsOn.asWorkerGroup.map(_.ref),
    runsOn = runsOn.asManagedResource.map(_.ref),
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref),
    attemptTimeout = attemptTimeout.map(_.toString),
    lateAfterTimeout = lateAfterTimeout.map(_.toString),
    maximumRetries = maximumRetries.map(_.toString),
    retryDelay = retryDelay.map(_.toString),
    failureAndRerunMode = failureAndRerunMode.map(_.toString)
  )

}

object RedshiftUnloadActivity extends RunnableObject {

  def apply(database: RedshiftDatabase, script: String, s3Path: Parameter[S3Uri],
    accessKeyId: StringParameter, accessKeySecret: StringParameter)(runsOn: Resource[Ec2Resource]): RedshiftUnloadActivity =
    new RedshiftUnloadActivity(
      id = PipelineObjectId(RedshiftUnloadActivity.getClass),
      script = script,
      s3Path = s3Path,
      database = database,
      queue = None,
      runsOn = runsOn,
      unloadOptions = Seq.empty,
      dependsOn = Seq.empty,
      preconditions = Seq.empty,
      onFailAlarms = Seq.empty,
      onSuccessAlarms = Seq.empty,
      onLateActionAlarms = Seq.empty,
      accessKeyId = accessKeyId,
      accessKeySecret = accessKeySecret,
      attemptTimeout = None,
      lateAfterTimeout = None,
      maximumRetries = None,
      retryDelay = None,
      failureAndRerunMode = None
    )

}
