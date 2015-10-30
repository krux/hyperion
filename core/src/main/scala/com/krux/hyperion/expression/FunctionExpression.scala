package com.krux.hyperion.expression

trait FunctionExpression { this: TypedExpression =>

  def name: String

  def args: Seq[Expression]

  def content = s"$name(${args.map(_.content).mkString(",")})"

}

/**
 * Creates a String object that is the result of converting the specified DateTime using the
 * specified format string. Example: #{format(myDateTime,'YYYY-MM-dd HH:mm:ss z')}
 */
case class Format(myDateTime: DateTimeExp, myFormat: StringExp) extends FunctionExpression
  with StringExp {

  def name = "format"

  def args = Seq(myDateTime, myFormat)

}

/**
 * Creates a DateTime object, in UTC, with the specified year, month, and day, at midnight.
 * Example: #{makeDate(2011,5,24)}
 */
case class MakeDate(theYear: IntExp, theMonth: IntExp, theDay: IntExp) extends FunctionExpression
  with DateTimeExp {

  def name = "makeDate"

  def args = Seq(theYear, theMonth, theDay)

}

/**
 * Creates a DateTime object, in UTC, with the specified year, month, day, hour, and minute.
 * Example: #{makeDateTime(2011,5,24,14,21)}
 */
case class MakeDateTime(
    theYear: IntExp, theMonth: IntExp, theDay: IntExp, theHour: IntExp, theMinute: IntExp
  ) extends FunctionExpression with DateTimeExp {

  def name = "makeDateTime"

  def args = Seq(theYear, theMonth, theDay, theHour, theMinute)

}

/**
 * Gets the year of the DateTime value as an integer.
 * Example: #{year(myDateTime)}
 */
case class ExtractYear(myDateTime: DateTimeExp) extends FunctionExpression with IntExp {

  def name = "year"

  def args = Seq(myDateTime)

}

// Gets the month of the DateTime value as an integer.
// Example: #{month(myDateTime)}
case class ExtractMonth(myDateTime: DateTimeExp) extends FunctionExpression with IntExp {

  def name = "month"

  def args = Seq(myDateTime)

}

/**
 * Gets the day of the DateTime value as an integer.
 * Example: #{day(myDateTime)}
 */
case class ExtractDay(myDateTime: DateTimeExp) extends FunctionExpression with IntExp {

  def name = "day"

  def args = Seq(myDateTime)

}

/**
 * Gets the day of the year of the DateTime value as an integer.
 * Example: #{dayOfYear(myDateTime)}
 */
case class DayOfYear(myDateTime: DateTimeExp) extends FunctionExpression with IntExp {

  def name = "dayOfYear"

  def args = Seq(myDateTime)

}

/**
 * Gets the hour of the DateTime value as an integer.
 * Example: #{hour(myDateTime)}
 */
case class ExtractHour(myDateTime: DateTimeExp) extends FunctionExpression with IntExp {

  def name = "hour"

  def args = Seq(myDateTime)

}

/**
 * Gets the minute of the DateTime value as an integer.
 * Example: #{minute(myDateTime)}
 */
case class ExtractMinute(myDateTime: DateTimeExp) extends FunctionExpression with IntExp {

  def name = "minute"

  def args = Seq(myDateTime)

}

/**
 * Creates a DateTime object for the start of the month in the specified DateTime.
 * Example: #{firstOfMonth(myDateTime)}
 */
case class FirstOfMonth(myDateTime: DateTimeExp) extends FunctionExpression with DateTimeExp {

  def name = "firstOfMonth"

  def args = Seq(myDateTime)

}


/**
 * Creates a DateTime object for the next midnight, relative to the specified DateTime.
 * Example: #{midnight(myDateTime)}
 */
case class Midnight(myDateTime: DateTimeExp) extends FunctionExpression with DateTimeExp {

  def name = "midnight"

  def args = Seq(myDateTime)

}


/**
 * Creates a DateTime object for the previous Sunday, relative to the specified DateTime.
 * If the specified DateTime is a Sunday, the result is the specified DateTime.
 * Example: #{sunday(myDateTime)}
 */
case class Sunday(myDateTime: DateTimeExp) extends FunctionExpression with DateTimeExp {

  def name = "sunday"

  def args = Seq(myDateTime)

}


/**
 * Creates a DateTime object for the previous day, relative to the specified DateTime.
 * The result is the same as minusDays(1).
 * Example: #{yesterday(myDateTime)}
 */
case class Yesterday(myDateTime: DateTimeExp) extends FunctionExpression with DateTimeExp {

  def name = "yesterday"

  def args = Seq(myDateTime)

}

/**
 * Creates a DateTime object with the same date and time, but in the specified time zone,
 * and taking daylight savings time into account. For more information about time zones,
 * see http://joda-time.sourceforge.net/timezones.html.
 * Example: #{inTimeZone(myDateTime,'America/Los_Angeles')}
 */
case class InTimeZone(myDateTime: DateTimeExp, zone: StringExp) extends FunctionExpression with DateTimeExp {

  def name = "inTimeZone"

  def args = Seq(myDateTime, zone)

}

/**
 * Creates a DateTime object that is the result of subtracting the specified number of years
 * from the specified DateTime.
 * Example: #{minusYears(myDateTime,1)}
 */
case class MinusYears(myDateTime: DateTimeExp, daysToSub: IntExp) extends FunctionExpression with DateTimeExp {

  def name = "minusYears"

  def args = Seq(myDateTime, daysToSub)

}

/**
 * Creates a DateTime object that is the result of subtracting the specified number of months
 * from the specified DateTime.
 * Example: #{minusMonths(myDateTime,1)}
 */
case class MinusMonths(myDateTime: DateTimeExp, daysToSub: IntExp) extends FunctionExpression with DateTimeExp {

  def name = "minusMonths"

  def args = Seq(myDateTime, daysToSub)

}

/**
 * Creates a DateTime object that is the result of subtracting the specified number of weeks
 * from the specified DateTime.
 * Example: #{minusWeeks(myDateTime,1)}
 */
case class MinusWeeks(myDateTime: DateTimeExp, daysToSub: IntExp) extends FunctionExpression with DateTimeExp {

  def name = "minusWeeks"

  def args = Seq(myDateTime, daysToSub)

}

/**
 * Creates a DateTime object that is the result of subtracting the specified number of days
 * from the specified DateTime.
 * Example: #{minusDays(myDateTime,1)}
 */
case class MinusDays(myDateTime: DateTimeExp, daysToSub: IntExp) extends FunctionExpression with DateTimeExp {

  def name = "minusDays"

  def args = Seq(myDateTime, daysToSub)

}

/**
 * Creates a DateTime object that is the result of subtracting the specified number of hours
 * from the specified DateTime.
 * Example: #{minusHours(myDateTime,1)}
 */
case class MinusHours(myDateTime: DateTimeExp, daysToSub: IntExp) extends FunctionExpression with DateTimeExp {

  def name = "minusHours"

  def args = Seq(myDateTime, daysToSub)

}

/**
 * Creates a DateTime object that is the result of subtracting the specified number of minutes
 * from the specified DateTime.
 * Example: #{minusMinutes(myDateTime,1)}
 */
case class MinusMinutes(myDateTime: DateTimeExp, daysToSub: IntExp) extends FunctionExpression with DateTimeExp {

  def name = "minusMinutes"

  def args = Seq(myDateTime, daysToSub)

}

/**
 * Creates a DateTime object that is the result of adding the specified number of years
 * to the specified DateTime.
 * Example: #{plusYears(myDateTime,1)}
 */
case class PlusYears(myDateTime: DateTimeExp, yearsToAdd: IntExp) extends FunctionExpression with DateTimeExp {

  def name = "plusYears"

  def args = Seq(myDateTime, yearsToAdd)

}

/**
 * Creates a DateTime object that is the result of adding the specified number of months
 * to the specified DateTime.
 * Example: #{plusMonths(myDateTime,1)}
 */
case class PlusMonths(myDateTime: DateTimeExp, monthsToAdd: IntExp) extends FunctionExpression with DateTimeExp {

  def name = "plusMonths"

  def args = Seq(myDateTime, monthsToAdd)

}

/**
 * Creates a DateTime object that is the result of adding the specified number of weeks
 * to the specified DateTime.
 * Example: #{plusWeeks(myDateTime,1)}
 */
case class PlusWeeks(myDateTime: DateTimeExp, weeksToAdd: IntExp) extends FunctionExpression with DateTimeExp {

  def name = "plusWeeks"

  def args = Seq(myDateTime, weeksToAdd)

}

/**
 * Creates a DateTime object that is the result of adding the specified number of days
 * to the specified DateTime.
 * Example: #{plusDays(myDateTime,1)}
 */
case class PlusDays(myDateTime: DateTimeExp, daysToAdd: IntExp) extends FunctionExpression with DateTimeExp {

  def name = "plusDays"

  def args = Seq(myDateTime, daysToAdd)

}

/**
 * Creates a DateTime object that is the result of adding the specified number of hours
 * to the specified DateTime.
 * Example: #{plusHours(myDateTime,1)}
 */
case class PlusHours(myDateTime: DateTimeExp, hoursToAdd: IntExp) extends FunctionExpression with DateTimeExp {

  def name = "plusHours"

  def args = Seq(myDateTime, hoursToAdd)

}

/**
 * Creates a DateTime object that is the result of adding the specified number of minutes
 * to the specified DateTime.
 * Example: #{plusMinutes(myDateTime,1)}
 */
case class PlusMinutes(myDateTime: DateTimeExp, minutesToAdd: IntExp) extends FunctionExpression with DateTimeExp {

  def name = "plusMinutes"

  def args = Seq(myDateTime, minutesToAdd)

}
