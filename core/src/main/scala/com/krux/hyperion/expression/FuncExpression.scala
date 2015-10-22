package com.krux.hyperion.expression

trait FuncExpression extends Expression {

  def funcName: String

  def args: Seq[Expression]

  def content = s"$funcName(${args.map(_.content).mkString(",")})"

}

/**
 * Creates a String object that is the result of converting the specified DateTime using the
 * specified format string. Example: #{format(myDateTime,'YYYY-MM-dd HH:mm:ss z')}
 */
case class Format(myDateTime: DateTimeTypedExp, myFormat: String) extends FuncExpression
  with StringTypedExp {

  def funcName = "format"

  val formatStringExp = new Expression {
    def content = s"'$myFormat'"
  }

  def args = Seq(myDateTime, formatStringExp)

}

/**
 * Creates a DateTime object, in UTC, with the specified year, month, and day, at midnight.
 * Example: #{makeDate(2011,5,24)}
 */
case class MakeDate(theYear: IntTypedExp, theMonth: IntTypedExp, theDay: IntTypedExp) extends FuncExpression
  with DateTimeTypedExp {

  def funcName = "makeDate"

  def args = Seq(theYear, theMonth, theDay)

}

/**
 * Creates a DateTime object, in UTC, with the specified year, month, day, hour, and minute.
 * Example: #{makeDateTime(2011,5,24,14,21)}
 */
case class MakeDateTime(
    theYear: IntTypedExp, theMonth: IntTypedExp, theDay: IntTypedExp, theHour: IntTypedExp, theMinute: IntTypedExp
  ) extends FuncExpression with DateTimeTypedExp {

  def funcName = "makeDate"

  def args = Seq(theYear, theMonth, theDay, theHour, theMinute)

}

/**
 * Gets the year of the DateTime value as an integer.
 * Example: #{year(myDateTime)}
 */
case class ExtractYear(myDateTime: DateTimeTypedExp) extends FuncExpression with IntTypedExp {

  def funcName = "year"

  def args = Seq(myDateTime)

}

// Gets the month of the DateTime value as an integer.
// Example: #{month(myDateTime)}
case class ExtractMonth(myDateTime: DateTimeTypedExp) extends FuncExpression with IntTypedExp {

  def funcName = "month"

  def args = Seq(myDateTime)

}

/**
 * Gets the day of the DateTime value as an integer.
 * Example: #{day(myDateTime)}
 */
case class ExtractDay(myDateTime: DateTimeTypedExp) extends FuncExpression with IntTypedExp {

  def funcName = "day"

  def args = Seq(myDateTime)

}

/**
 * Gets the day of the year of the DateTime value as an integer.
 * Example: #{dayOfYear(myDateTime)}
 */
case class DayOfYear(myDateTime: DateTimeTypedExp) extends FuncExpression with IntTypedExp {

  def funcName = "dayOfYear"

  def args = Seq(myDateTime)

}

/**
 * Gets the hour of the DateTime value as an integer.
 * Example: #{hour(myDateTime)}
 */
case class ExtractHour(myDateTime: DateTimeTypedExp) extends FuncExpression with IntTypedExp {

  def funcName = "hour"

  def args = Seq(myDateTime)

}

/**
 * Gets the minute of the DateTime value as an integer.
 * Example: #{minute(myDateTime)}
 */
case class ExtractMinute(myDateTime: DateTimeTypedExp) extends FuncExpression with IntTypedExp {

  def funcName = "minute"

  def args = Seq(myDateTime)

}

/**
 * Creates a DateTime object for the start of the month in the specified DateTime.
 * Example: #{firstOfMonth(myDateTime)}
 */
case class FirstOfMonth(myDateTime: DateTimeTypedExp) extends FuncExpression with DateTimeTypedExp {

  def funcName = "firstOfMonth"

  def args = Seq(myDateTime)

}


/**
 * Creates a DateTime object for the next midnight, relative to the specified DateTime.
 * Example: #{midnight(myDateTime)}
 */
case class Midnight(myDateTime: DateTimeTypedExp) extends FuncExpression with DateTimeTypedExp {

  def funcName = "midnight"

  def args = Seq(myDateTime)

}


/**
 * Creates a DateTime object for the previous Sunday, relative to the specified DateTime.
 * If the specified DateTime is a Sunday, the result is the specified DateTime.
 * Example: #{sunday(myDateTime)}
 */
case class Sunday(myDateTime: DateTimeTypedExp) extends FuncExpression with DateTimeTypedExp {

  def funcName = "sunday"

  def args = Seq(myDateTime)

}


/**
 * Creates a DateTime object for the previous day, relative to the specified DateTime.
 * The result is the same as minusDays(1).
 * Example: #{yesterday(myDateTime)}
 */
case class Yesterday(myDateTime: DateTimeTypedExp) extends FuncExpression with DateTimeTypedExp {

  def funcName = "yesterday"

  def args = Seq(myDateTime)

}

/**
 * Creates a DateTime object with the same date and time, but in the specified time zone,
 * and taking daylight savings time into account. For more information about time zones,
 * see http://joda-time.sourceforge.net/timezones.html.
 * Example: #{inTimeZone(myDateTime,'America/Los_Angeles')}
 */
case class InTimeZone(myDateTime: DateTimeTypedExp, zone: String) extends FuncExpression with DateTimeTypedExp {

  def funcName = "inTimeZone"

  val zoneStringExp = new Expression {
    def content = s"'$zone'"
  }

  def args = Seq(myDateTime, zoneStringExp)

}

/**
 * Creates a DateTime object that is the result of subtracting the specified number of years
 * from the specified DateTime.
 * Example: #{minusYears(myDateTime,1)}
 */
case class MinusYears(myDateTime: DateTimeTypedExp, daysToSub: IntTypedExp) extends FuncExpression with DateTimeTypedExp {

  def funcName = "minusYears"

  def args = Seq(myDateTime, daysToSub)

}

/**
 * Creates a DateTime object that is the result of subtracting the specified number of months
 * from the specified DateTime.
 * Example: #{minusMonths(myDateTime,1)}
 */
case class MinusMonths(myDateTime: DateTimeTypedExp, daysToSub: IntTypedExp) extends FuncExpression with DateTimeTypedExp {

  def funcName = "minusMonths"

  def args = Seq(myDateTime, daysToSub)

}

/**
 * Creates a DateTime object that is the result of subtracting the specified number of weeks
 * from the specified DateTime.
 * Example: #{minusWeeks(myDateTime,1)}
 */
case class MinusWeeks(myDateTime: DateTimeTypedExp, daysToSub: IntTypedExp) extends FuncExpression with DateTimeTypedExp {

  def funcName = "minusWeeks"

  def args = Seq(myDateTime, daysToSub)

}

/**
 * Creates a DateTime object that is the result of subtracting the specified number of days
 * from the specified DateTime.
 * Example: #{minusDays(myDateTime,1)}
 */
case class MinusDays(myDateTime: DateTimeTypedExp, daysToSub: IntTypedExp) extends FuncExpression with DateTimeTypedExp {

  def funcName = "minusDays"

  def args = Seq(myDateTime, daysToSub)

}

/**
 * Creates a DateTime object that is the result of subtracting the specified number of hours
 * from the specified DateTime.
 * Example: #{minusHours(myDateTime,1)}
 */
case class MinusHours(myDateTime: DateTimeTypedExp, daysToSub: IntTypedExp) extends FuncExpression with DateTimeTypedExp {

  def funcName = "minusHours"

  def args = Seq(myDateTime, daysToSub)

}

/**
 * Creates a DateTime object that is the result of subtracting the specified number of minutes
 * from the specified DateTime.
 * Example: #{minusMinutes(myDateTime,1)}
 */
case class MinusMinutes(myDateTime: DateTimeTypedExp, daysToSub: IntTypedExp) extends FuncExpression with DateTimeTypedExp {

  def funcName = "minusMinutes"

  def args = Seq(myDateTime, daysToSub)

}

/**
 * Creates a DateTime object that is the result of adding the specified number of years
 * to the specified DateTime.
 * Example: #{plusYears(myDateTime,1)}
 */
case class PlusYears(myDateTime: DateTimeTypedExp, yearsToAdd: IntTypedExp) extends FuncExpression with DateTimeTypedExp {

  def funcName = "plusYears"

  def args = Seq(myDateTime, yearsToAdd)

}

/**
 * Creates a DateTime object that is the result of adding the specified number of months
 * to the specified DateTime.
 * Example: #{plusMonths(myDateTime,1)}
 */
case class PlusMonths(myDateTime: DateTimeTypedExp, monthsToAdd: IntTypedExp) extends FuncExpression with DateTimeTypedExp {

  def funcName = "plusMonths"

  def args = Seq(myDateTime, monthsToAdd)

}

/**
 * Creates a DateTime object that is the result of adding the specified number of weeks
 * to the specified DateTime.
 * Example: #{plusWeeks(myDateTime,1)}
 */
case class PlusWeeks(myDateTime: DateTimeTypedExp, weeksToAdd: IntTypedExp) extends FuncExpression with DateTimeTypedExp {

  def funcName = "plusWeeks"

  def args = Seq(myDateTime, weeksToAdd)

}

/**
 * Creates a DateTime object that is the result of adding the specified number of days
 * to the specified DateTime.
 * Example: #{plusDays(myDateTime,1)}
 */
case class PlusDays(myDateTime: DateTimeTypedExp, daysToAdd: IntTypedExp) extends FuncExpression with DateTimeTypedExp {

  def funcName = "plusDays"

  def args = Seq(myDateTime, daysToAdd)

}

/**
 * Creates a DateTime object that is the result of adding the specified number of hours
 * to the specified DateTime.
 * Example: #{plusHours(myDateTime,1)}
 */
case class PlusHours(myDateTime: DateTimeTypedExp, hoursToAdd: IntTypedExp) extends FuncExpression with DateTimeTypedExp {

  def funcName = "plusHours"

  def args = Seq(myDateTime, hoursToAdd)

}

/**
 * Creates a DateTime object that is the result of adding the specified number of minutes
 * to the specified DateTime.
 * Example: #{plusMinutes(myDateTime,1)}
 */
case class PlusMinutes(myDateTime: DateTimeTypedExp, minutesToAdd: IntTypedExp) extends FuncExpression with DateTimeTypedExp {

  def funcName = "plusMinutes"

  def args = Seq(myDateTime, minutesToAdd)

}
