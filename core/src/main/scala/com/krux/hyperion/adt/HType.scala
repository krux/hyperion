package com.krux.hyperion.adt

import scala.language.implicitConversions

import org.joda.time.{DateTimeZone, DateTime}

import com.krux.hyperion.expression._
import com.krux.hyperion.common.{HdfsUri, S3Uri, OptionalOrdered}

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
  implicit def stringOption2HStringOption(value: Option[String]): Option[HString] =
    value.map(v => HString(Left(v)))
  implicit def stringExp2HString(value: StringExp): HString = HString(Right(value))

  implicit def int2HInt(value: Int): HInt = HInt(Left(value))
  implicit def intExp2HInt(value: IntExp): HInt = HInt(Right(value))

  implicit def double2HDouble(value: Double): HDouble = HDouble(Left(value))
  implicit def doubleExp2HDouble(value: DoubleExp): HDouble = HDouble(Right(value))

  implicit def boolean2HBoolean(value: Boolean): HBoolean = HBoolean(Left(value))
  implicit def booleanExp2HBoolean(value: BooleanExp): HBoolean = HBoolean(Right(value))

  implicit def dateTime2HDateTime(value: DateTime): HDateTime = HDateTime(Left(value))
  implicit def dateTimeExp2HDateTime(value: DateTimeExp): HDateTime = HDateTime(Right(value))

  implicit def duration2HDuration(value: Duration): HDuration = HDuration(Left(value))
  implicit def durationExp2HDuration(value: DurationExp): HDuration = HDuration(Right(value))

  implicit def s3Uri2HS3Uri(value: S3Uri): HS3Uri = HS3Uri(Left(value))
  implicit def s3UriExp2HS3Uri(value: S3UriExp): HS3Uri = HS3Uri(Right(value))

  implicit def hdfsUri2HHdfsUri(value: HdfsUri): HHdfsUri = HHdfsUri(Left(value))
  implicit def hdfsUriExp2HHdfsUri(value: HdfsUriExp): HHdfsUri = HHdfsUri(Right(value))

  implicit def long2HLong(value: Long): HLong = HLong(Left(value))
  implicit def longExp2HLong(value: LongExp): HLong = HLong(Right(value))

}

case class HString(value: Either[String, StringExp]) extends HType

case class HInt(value: Either[Int, IntExp]) extends HType with OptionalOrdered[Int] {

  def isZero: Option[Boolean] = value match {
    case Left(v) => Option(v == 0)
    case _ => None
  }

  def compare(that: Int): Option[Int] = value match {
    case Left(v) => Some(v - that)
    case Right(v) =>
      v match {
        case x: Evaluatable[_] => Some(x.evaluate().asInstanceOf[Int] - that)
        case _ => None
      }
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

case class HLong(value: Either[Long, LongExp]) extends HType with OptionalOrdered[Long] {

  def compare(that: Long): Option[Int] = value match {
    case Left(v) => Some(java.lang.Long.compare(v, that))
    case Right(v) =>
      v match {
        case x: Evaluatable[_] =>
          Some(java.lang.Long.compare(x.evaluate().asInstanceOf[Long], that))
        case _ =>
          None
      }
  }

}

case class HDouble(value: Either[Double, DoubleExp]) extends HType with OptionalOrdered[Double] {

  def compare(that: Double): Option[Int] = value match {
    case Left(v) => Some(java.lang.Double.compare(v, that))
    case Right(v) =>
      v match {
        case x: Evaluatable[_] =>
          Some(java.lang.Double.compare(x.evaluate().asInstanceOf[Double], that))
        case _ =>
          None
      }
  }

}

case class HBoolean(value: Either[Boolean, BooleanExp]) extends HType {
  def exists[B](fn: => B) = value match {
    case Left(true) => Option(fn)
    case _ => None
  }
}

object HBoolean {
  final val True = HBoolean(Left(true))
  final val False = HBoolean(Left(false))

  implicit def hboolean2Boolean(b: HBoolean): Boolean = b.value match {
    case Left(v) => v
    case Right(v) => v.evaluate()
  }
}

case class HDateTime(value: Either[DateTime, DateTimeExp]) extends HType {

  val datetimeFormat = "yyyy-MM-dd'T'HH:mm:ss"

  override def serialize: String = value match {
    case Left(dt) => dt.toDateTime(DateTimeZone.UTC).toString(datetimeFormat)
    case Right(expr) => expr.toString
  }

}

object HDateTime {

  implicit def hDateTime2DateTimeExp(dt: HDateTime): DateTimeExp = dt.value match {
    case Left(x) => DateTimeConstantExp(x)
    case Right(x) => x
  }

}

case class HDuration(value: Either[Duration, DurationExp]) extends HType

case class HS3Uri(value: Either[S3Uri, S3UriExp]) extends HType

case class HHdfsUri(value: Either[HdfsUri, HdfsUriExp]) extends HType