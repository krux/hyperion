package com.krux.hyperion.expression

import scala.language.implicitConversions

import org.joda.time.{DateTimeZone, DateTime}

trait ValExpression extends Expression

case class StringExp(raw: String) extends ValExpression with StringTypedExp {

  def content: String = s"""\"$raw\""""

}

case class IntExp(num: Int) extends ValExpression with IntTypedExp {

  def content: String = num.toString

}

case class DuobleExp(num: Double) extends ValExpression with DoubleTypedExp {

  def content: String = num.toString

}

case class DateTimeExp(dt: DateTime) extends ValExpression with DateTimeTypedExp {

  implicit def int2IntExp(n: Int): IntExp = IntExp(n)

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
