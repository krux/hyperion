package com.krux.hyperion.expression

import scala.language.implicitConversions

import org.joda.time.{DateTimeZone, DateTime}

trait ConstantExpression[T] extends Expression with Evaluatable[T] {
  def constantValue: T
  def content: String = constantValue.toString

  def evaluate(): T = constantValue
}

object ConstantExpression {

  implicit def string2StringConstantExp(raw: String): StringConstantExp = StringConstantExp(raw)
  implicit def int2IntConstantExp(num: Int): IntConstantExp = IntConstantExp(num)
  implicit def double2DoubleConstantExp(num: Double): DoubleConstantExp = DoubleConstantExp(num)
  implicit def dateTime2DateTimeConstantExp(dt: DateTime): DateTimeConstantExp = DateTimeConstantExp(dt)

}

case class StringConstantExp(constantValue: String) extends ConstantExpression[String] with StringExp {

  override def content: String = s"""\"$constantValue\""""

}

case class IntConstantExp(constantValue: Int) extends ConstantExpression[Int] with IntExp

case class LongConstantExp(constantValue: Int) extends ConstantExpression[Int] with LongExp

case class DoubleConstantExp(constantValue: Double) extends ConstantExpression[Double] with DoubleExp

case class DateTimeConstantExp(constantValue: DateTime) extends ConstantExpression[DateTime] with DateTimeExp {

  import ConstantExpression._

  override def content: String = {

    val utc = constantValue.toDateTime(DateTimeZone.UTC)

    val funcDt =
      if (utc.getHourOfDay == 0 && utc.getMinuteOfHour == 0)
        MakeDate(utc.getYear, utc.getMonthOfYear, utc.getDayOfMonth)
      else
        MakeDateTime(
          utc.getYear,
          utc.getMonthOfYear,
          utc.getDayOfMonth,
          utc.getHourOfDay,
          utc.getMinuteOfHour)

    funcDt.content

  }

}

case class BooleanConstantExp(constantValue: Boolean) extends ConstantExpression[Boolean] with BooleanExp
