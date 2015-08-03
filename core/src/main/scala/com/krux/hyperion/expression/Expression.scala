package com.krux.hyperion.expression

import org.joda.time.{DateTimeZone, DateTime}

import scala.language.implicitConversions

/**
 * Expression. Expressions are delimited by: "#{" and "}" and the contents of the braces are
 * evaluated by AWS Data Pipeline.
 */
sealed trait Expression {

  def content: String

  override def toString = s"#{$content}"

}

/**
 * For expressions that return strings.
 */
case class StringExp(content: String) extends Expression {

  def + (e: StringExp) = StringExp(s"$content + ${e.content}")

}

/**
 * For expressions that return numbers.
 */
case class NumericExp(content: String) extends Expression {

  def + (e: NumericExp) = NumericExp(s"$content + ${e.content}")
  def - (e: NumericExp) = NumericExp(s"$content - ${e.content}")
  def * (e: NumericExp) = NumericExp(s"$content * ${e.content}")
  def / (e: NumericExp) = NumericExp(s"$content / ${e.content}")
  def ^ (e: NumericExp) = NumericExp(s"$content ^ ${e.content}")

}

object NumericExp {
  implicit def intToNumericExp(n: Int): NumericExp = NumericExp(n.toString)
}

/**
 * For expressions that returns DateTimes.
 */
case class DateTimeExp(content: String) extends Expression {

  def + (period: Duration): DateTimeExp = period match {
    case Minute(n) => DateTimeFunctions.plusMinutes(this, n)
    case Hour(n) => DateTimeFunctions.plusHours(this, n)
    case Day(n) => DateTimeFunctions.plusDays(this, n)
    case Week(n) => DateTimeFunctions.plusWeeks(this, n)
    case Month(n) => DateTimeFunctions.plusMonths(this, n)
    case Year(n) => DateTimeFunctions.plusYears(this, n)
  }

  def - (period: Duration): DateTimeExp = period match {
    case Minute(n) => DateTimeFunctions.minusMinutes(this, n)
    case Hour(n) => DateTimeFunctions.minusHours(this, n)
    case Day(n) => DateTimeFunctions.minusDays(this, n)
    case Week(n) => DateTimeFunctions.minusWeeks(this, n)
    case Month(n) => DateTimeFunctions.minusMonths(this, n)
    case Year(n) => DateTimeFunctions.minusYears(this, n)
  }

  def year: NumericExp = DateTimeFunctions.year(this)
  def month: NumericExp = DateTimeFunctions.year(this)
  def day: NumericExp = DateTimeFunctions.year(this)
  def dayOfYear: NumericExp = DateTimeFunctions.year(this)
  def hour: NumericExp = DateTimeFunctions.year(this)
  def minute: NumericExp = DateTimeFunctions.year(this)

  def firstOfMonth: NumericExp = DateTimeFunctions.year(this)
  def midnight: NumericExp = DateTimeFunctions.year(this)
  def sunday: NumericExp = DateTimeFunctions.year(this)
  def yesterday: NumericExp = DateTimeFunctions.year(this)
  def inTimeZone(zone: String) = DateTimeFunctions.inTimeZone(this, zone)

}

object DateTimeExp {

  def apply(dt: DateTime): DateTimeExp = {
    val utc = dt.toDateTime(DateTimeZone.UTC)
    if (utc.getHourOfDay > 0 || utc.getMinuteOfHour > 0)
      DateTimeFunctions.makeDateTime(utc.getYear, utc.getMonthOfYear, utc.getDayOfMonth, utc.getHourOfDay, utc.getMinuteOfHour)
    else
      DateTimeFunctions.makeDate(utc.getYear, utc.getMonthOfYear, utc.getDayOfMonth)
  }

  def apply(year: NumericExp, month: NumericExp, day: NumericExp): DateTimeExp =
    DateTimeFunctions.makeDate(year, month, day)

  def apply(year: NumericExp, month: NumericExp, day: NumericExp, hour: NumericExp, minute: NumericExp): DateTimeExp =
    DateTimeFunctions.makeDateTime(year, month, day, hour, minute)

}
