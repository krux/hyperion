package com.krux.hyperion.expression

import scala.language.implicitConversions

import org.joda.time.{DateTimeZone, DateTime}

trait ConstantExpression extends Expression

object ConstantExpression {

  implicit def string2StringConstantExp(raw: String): StringConstantExp = StringConstantExp(raw)
  implicit def int2IntConstantExp(num: Int): IntConstantExp = IntConstantExp(num)
  implicit def double2DoubleConstantExp(num: Double): DoubleConstantExp = DoubleConstantExp(num)
  implicit def dateTime2DateTimeConstantExp(dt: DateTime): DateTimeConstantExp = DateTimeConstantExp(dt)

}

case class StringConstantExp(raw: String) extends ConstantExpression with StringExp {

  def content: String = s"""\"$raw\""""

}

case class IntConstantExp(num: Int) extends ConstantExpression with IntExp {

  def content: String = num.toString

}

case class DoubleConstantExp(num: Double) extends ConstantExpression with DoubleExp {

  def content: String = num.toString

}

case class DateTimeConstantExp(dt: DateTime) extends ConstantExpression with DateTimeExp {

  implicit def int2IntExp(n: Int): IntConstantExp = IntConstantExp(n)

  def content: String = {

    val utc = dt.toDateTime(DateTimeZone.UTC)

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
