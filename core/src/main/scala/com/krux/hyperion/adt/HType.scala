package com.krux.hyperion.adt

import scala.language.implicitConversions

import org.joda.time.{DateTimeZone, DateTime}

import com.krux.hyperion.expression.{DateTimeExp, IntExp, StringExp, DoubleExp,
  TypedExpression, IntConstantExp, DurationExp, Duration, S3UriExp, BooleanExp}
import com.krux.hyperion.common.S3Uri

sealed abstract class HType {

  def value: Either[Any, TypedExpression]

  def serialize: String = value match {
    case Left(v) => v.toString
    case Right(r) => r.serialize
  }

  override def toString = serialize

}

object HType {

  implicit def string2HString(value: String): HString = HString(Left(value))
  implicit def stringTypedExp2HString(value: StringExp): HString = HString(Right(value))

  implicit def int2HInt(value: Int): HInt = HInt(Left(value))
  implicit def intTypedExp2HInt(value: IntExp): HInt = HInt(Right(value))

  implicit def double2HDouble(value: Double): HDouble = HDouble(Left(value))
  implicit def doubleTypedExp2HDouble(value: DoubleExp): HDouble = HDouble(Right(value))

  implicit def boolean2HBoolean(value: Boolean): HBoolean = HBoolean(Left(value))
  implicit def booleanTypedExp2HBoolean(value: BooleanExp): HBoolean = HBoolean(Right(value))

  implicit def dateTime2HDateTime(value: DateTime): HDateTime = HDateTime(Left(value))
  implicit def dateTimeTypedExp2HDateTime(value: DateTimeExp): HDateTime = HDateTime(Right(value))

  implicit def duration2HDuration(value: Duration): HDuration = HDuration(Left(value))
  implicit def durationTypedExp2HDuration(value: DurationExp): HDuration = HDuration(Right(value))

  implicit def s3Uri2HS3Uri(value: S3Uri): HS3Uri = HS3Uri(Left(value))
  implicit def s3UriTypedExp2HS3Uri(value: S3UriExp): HS3Uri = HS3Uri(Right(value))

}

case class HString(value: Either[String, StringExp]) extends HType

case class HInt(value: Either[Int, IntExp]) extends HType with Ordered[Int] {

  def isZero: Option[Boolean] = value match {
    case Left(v) => Option(v == 0)
    case _ => None
  }

  def compare(that: Int): Int = value match {
    case Left(v) => v - that
    case _ => throw new NotComparableException("Cannot compare expressions")
  }

  def + (that: HInt): HInt = this.value match {
    case Left(i) => that.value match {
      case Left(j) => HInt(Left(i + j))
      case Right(j) => HInt(Right(IntConstantExp(i) + j))
    }
    case Right(i) => that.value match {
      case Left(j) => HInt(Right(i + IntConstantExp(j)))
      case Right(j) => HInt(Right(i + j))
    }
  }

}

case class HDouble(value: Either[Double, DoubleExp]) extends HType with Ordered[Double] {

  def compare(that: Double): Int = value match {
    case Left(v) => java.lang.Double.compare(v, that)
    case _ => throw new NotComparableException("Cannot cmpare expressions")
  }

}

case class HBoolean(value: Either[Boolean, BooleanExp]) extends HType

object HBoolean {
  final val True = HBoolean(Left(true))
  final val False = HBoolean(Left(false))
}

case class HDateTime(value: Either[DateTime, DateTimeExp]) extends HType {

  val datetimeFormat = "yyyy-MM-dd'T'HH:mm:ss"

  override def serialize: String = value match {
    case Left(dt) => dt.toDateTime(DateTimeZone.UTC).toString(datetimeFormat)
    case Right(expr) => expr.toString
  }

}

case class HDuration(value: Either[Duration, DurationExp]) extends HType

case class HS3Uri(value: Either[S3Uri, S3UriExp]) extends HType
