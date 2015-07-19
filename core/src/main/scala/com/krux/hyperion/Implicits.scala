package com.krux.hyperion

import com.github.nscala_time.time.Imports._
import com.krux.hyperion.common.PipelineObjectId
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.DpPeriodBuilder
import com.krux.hyperion.expression.{DateTimeRef, DateTimeExp, Expression}
import scala.language.implicitConversions
import org.json4s.DefaultFormats

/**
 * The implicit conversions used in datapipeline
 */
object Implicits {

  implicit def string2DateTime(day: String): DateTime = new DateTime(day)
  implicit val jsonFormats = DefaultFormats

  // Expression implicit
  implicit def int2DpPeriod(n: Int): DpPeriodBuilder = new DpPeriodBuilder(n)

  implicit def dateTimeRef2dateTimeExp(dtRef: DateTimeRef.Value): DateTimeExp =
    new DateTimeExp(dtRef.toString)

  implicit def expression2String(exp: Expression): String = exp.toString

  // Convert to relevant PipelineObject
  implicit def string2S3DataNode(s3path: String): S3DataNode =
    S3DataNode.fromPath(s3path)

  implicit def string2UniquePipelineId(prefix: String): PipelineObjectId =
    PipelineObjectId(prefix)

}
