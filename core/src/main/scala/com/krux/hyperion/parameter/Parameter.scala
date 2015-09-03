package com.krux.hyperion.parameter

import scala.language.implicitConversions

import com.krux.hyperion.expression.Duration
import com.krux.hyperion.common.S3Uri
import com.krux.hyperion.aws.AdpParameter

trait Parameter[T] {
  def id: String
  def description: Option[String]
  def isEncrypted: Boolean
  def value: T

  def name = if (isEncrypted) s"*my_$id" else s"my_$id"

  def serialize: Option[AdpParameter]

  override def toString = s"#{$name}"
}

case class DirectValueParameter[T](value: T) extends Parameter[T] {

  val id = value.toString
  val description = None
  val isEncrypted = false
  override val name = value.toString
  override def toString = value.toString
  def serialize = None

}

object Parameter {

  implicit def booleanToParameter(b: Boolean): Parameter[Boolean] = DirectValueParameter[Boolean](b)
  implicit def doubleToParameter(d: Double): Parameter[Double] = DirectValueParameter[Double](d)
  implicit def durationToParameter(d: Duration): Parameter[Duration] = DirectValueParameter[Duration](d)
  implicit def intToParameter(i: Int): Parameter[Int] = DirectValueParameter[Int](i)
  implicit def longToParameter(i: Long): Parameter[Long] = DirectValueParameter[Long](i)
  implicit def s3UriToParameter(uri: S3Uri): Parameter[S3Uri] = DirectValueParameter[S3Uri](uri)
  implicit def stringToS3Parameter(s: String): Parameter[S3Uri] = DirectValueParameter[S3Uri](S3Uri(s))
  implicit def stringToParameter(s: String): Parameter[String] = DirectValueParameter[String](s)

  implicit def parameterToString(p: Parameter[_]): String = p.toString

}
