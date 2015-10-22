package com.krux.hyperion.adt

import scala.language.implicitConversions

import org.joda.time.{DateTimeZone, DateTime}

import com.krux.hyperion.expression.{DateTimeTypedExp, IntTypedExp, StringTypedExp, DoubleTypedExp,
  TypedExpression, IntExp, DurationTypedExp, Duration, S3UriTypedExp, BooleanTypedExp}
import com.krux.hyperion.common.S3Uri

sealed abstract class HType {

  def value: Either[Any, TypedExpression]

  def toAws: String = value match {
    case Left(v) => v.toString
    case Right(r) => r.toAws
  }

  override def toString = toAws

}

object HType {

  implicit def string2HString(value: String): HString = HString(Left(value))
  implicit def stringTypedExp2HString(value: StringTypedExp): HString = HString(Right(value))

  implicit def int2HInt(value: Int): HInt = HInt(Left(value))
  implicit def intTypedExp2HInt(value: IntTypedExp): HInt = HInt(Right(value))

  implicit def double2HDouble(value: Double): HDouble = HDouble(Left(value))
  implicit def doubleTypedExp2HDouble(value: DoubleTypedExp): HDouble = HDouble(Right(value))

  implicit def boolean2HBoolean(value: Boolean): HBoolean = HBoolean(Left(value))
  implicit def booleanTypedExp2HBoolean(value: BooleanTypedExp): HBoolean = HBoolean(Right(value))

  implicit def dateTime2HDateTime(value: DateTime): HDateTime = HDateTime(Left(value))
  implicit def dateTimeTypedExp2HDateTime(value: DateTimeTypedExp): HDateTime = HDateTime(Right(value))

  implicit def duration2HDuration(value: Duration): HDuration = HDuration(Left(value))
  implicit def durationTypedExp2HDuration(value: DurationTypedExp): HDuration = HDuration(Right(value))

  implicit def s3Uri2HS3Uri(value: S3Uri): HS3Uri = HS3Uri(Left(value))
  implicit def s3UriTypedExp2HS3Uri(value: S3UriTypedExp): HS3Uri = HS3Uri(Right(value))

}

case class HString(value: Either[String, StringTypedExp]) extends HType

case class HInt(value: Either[Int, IntTypedExp]) extends HType {

  def isZero: Option[Boolean] = value match {
    case Left(v) => Option(v == 0)
    case _ => None
  }

  def >= (i: Int): Option[Boolean] = value match {
    case Left(v) => Option(v >= i)
    case _ => None
  }

  def + (that: HInt): HInt = this.value match {
    case Left(i) => that.value match {
      case Left(j) => HInt(Left(i + j))
      case Right(j) => HInt(Right(IntExp(i) + j))
    }
    case Right(i) => that.value match {
      case Left(j) => HInt(Right(i + IntExp(j)))
      case Right(j) => HInt(Right(i + j))
    }
  }

}

case class HDouble(value: Either[Double, DoubleTypedExp]) extends HType

case class HBoolean(value: Either[Boolean, BooleanTypedExp]) extends HType

object HBoolean {
  final val True = HBoolean(Left(true))
  final val False = HBoolean(Left(false))
}

case class HDateTime(value: Either[DateTime, DateTimeTypedExp]) extends HType {

  val datetimeFormat = "yyyy-MM-dd'T'HH:mm:ss"

  override def toAws: String = value match {
    case Left(dt) => dt.toDateTime(DateTimeZone.UTC).toString(datetimeFormat)
    case Right(expr) => expr.toString
  }

}

case class HDuration(value: Either[Duration, DurationTypedExp]) extends HType

case class HS3Uri(value: Either[S3Uri, S3UriTypedExp]) extends HType
