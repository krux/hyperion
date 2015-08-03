package com.krux.hyperion.expression

/**
 * This implements the AWS DataPipeline Date and Time Functions
 *
 * ref: http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-pipeline-reference-functions-datetime.html
 */
trait DateTimeFunctions {

  // Creates a String object that is the result of converting the specified DateTime using the specified format string.
  // Example: #{format(myDateTime,'YYYY-MM-dd HH:mm:ss z')}
  def format(myDateTime: DateTimeExp, myFormat: String) =
    StringExp(s"format(${myDateTime.content},'$myFormat')")

  // Creates a DateTime object, in UTC, with the specified year, month, and day, at midnight.
  // Example: #{makeDate(2011,5,24)}
  def makeDate(year: NumericExp, month: NumericExp, day: NumericExp) =
    genDateTimeSubFunc("makeDate", year.content, month.content, day.content)

  // Creates a DateTime object, in UTC, with the specified year, month, day, hour, and minute.
  // Example: #{makeDateTime(2011,5,24,14,21)}
  def makeDateTime(year: NumericExp, month: NumericExp, day: NumericExp, hour: NumericExp, minute: NumericExp) =
    genDateTimeSubFunc("makeDate", year.content, month.content, day.content, hour.content, minute.content)

  // Gets the year of the DateTime value as an integer.
  // Example: #{year(myDateTime)}
  def year(myDateTime: DateTimeExp) =
    genNumericSubFunc("year", myDateTime.content)

  // Gets the month of the DateTime value as an integer.
  // Example: #{month(myDateTime)}
  def month(myDateTime: DateTimeExp) =
    genNumericSubFunc("month", myDateTime.content)

  // Gets the day of the DateTime value as an integer.
  // Example: #{day(myDateTime)}
  def day(myDateTime: DateTimeExp) =
    genNumericSubFunc("day", myDateTime.content)

  // Gets the day of the year of the DateTime value as an integer.
  // Example: #{dayOfYear(myDateTime)}
  def dayOfYear(myDateTime: DateTimeExp) =
    genNumericSubFunc("dayOfYear", myDateTime.content)

  // Gets the hour of the DateTime value as an integer.
  // Example: #{hour(myDateTime)}
  def hour(myDateTime: DateTimeExp) =
    genNumericSubFunc("hour", myDateTime.content)

  // Gets the minute of the DateTime value as an integer.
  // Example: #{minute(myDateTime)}
  def minute(myDateTime: DateTimeExp) =
    genNumericSubFunc("minute", myDateTime.content)

  // Creates a DateTime object for the start of the month in the specified DateTime.
  // Example: #{firstOfMonth(myDateTime)}
  def firstOfMonth(myDateTime: DateTimeExp) =
    genDateTimeSubFunc("firstOfMonth", myDateTime.content)

  // Creates a DateTime object for the next midnight, relative to the specified DateTime.
  // Example: #{midnight(myDateTime)}
  def midnight(myDateTime: DateTimeExp) =
    genDateTimeSubFunc("midnight", myDateTime.content)

  // Creates a DateTime object for the previous Sunday, relative to the specified DateTime.
  // If the specified DateTime is a Sunday, the result is the specified DateTime.
  // Example: #{sunday(myDateTime)}
  def sunday(myDateTime: DateTimeExp) =
    genDateTimeSubFunc("sunday", myDateTime.content)

  // Creates a DateTime object for the previous day, relative to the specified DateTime.
  // The result is the same as minusDays(1).
  // Example: #{yesterday(myDateTime)}
  def yesterday(myDateTime: DateTimeExp) =
    genDateTimeSubFunc("yesterday", myDateTime.content)

  // Creates a DateTime object with the same date and time, but in the specified time zone,
  // and taking daylight savings time into account. For more information about time zones,
  // see http://joda-time.sourceforge.net/timezones.html.
  // Example: #{inTimeZone(myDateTime,'America/Los_Angeles')}
  def inTimeZone(myDateTime: DateTimeExp, zone: String) =
    genDateTimeSubFunc("inTimeZone", myDateTime.content, zone)

  // Creates a DateTime object that is the result of subtracting the specified number of years
  // from the specified DateTime.
  // Example: #{minusYears(myDateTime,1)}
  def minusYears(myDateTime: DateTimeExp, daysToSub: NumericExp) =
    genDateTimeSubFunc("minusYears", myDateTime.content, daysToSub.content)

  // Creates a DateTime object that is the result of subtracting the specified number of months
  // from the specified DateTime.
  // Example: #{minusMonths(myDateTime,1)}
  def minusMonths(myDateTime: DateTimeExp, daysToSub: NumericExp) =
    genDateTimeSubFunc("minusMonths", myDateTime.content, daysToSub.content)

  // Creates a DateTime object that is the result of subtracting the specified number of weeks
  // from the specified DateTime.
  // Example: #{minusWeeks(myDateTime,1)}
  def minusWeeks(myDateTime: DateTimeExp, daysToSub: NumericExp) =
    genDateTimeSubFunc("minusWeeks", myDateTime.content, daysToSub.content)

  // Creates a DateTime object that is the result of subtracting the specified number of days
  // from the specified DateTime.
  // Example: #{minusDays(myDateTime,1)}
  def minusDays(myDateTime: DateTimeExp, daysToSub: NumericExp) =
    genDateTimeSubFunc("minusDays", myDateTime.content, daysToSub.content)

  // Creates a DateTime object that is the result of subtracting the specified number of hours
  // from the specified DateTime.
  // Example: #{minusHours(myDateTime,1)}
  def minusHours(myDateTime: DateTimeExp, daysToSub: NumericExp) =
    genDateTimeSubFunc("minusHours", myDateTime.content, daysToSub.content)

  // Creates a DateTime object that is the result of subtracting the specified number of minutes
  // from the specified DateTime.
  // Example: #{minusMinutes(myDateTime,1)}
  def minusMinutes(myDateTime: DateTimeExp, daysToSub: NumericExp) =
    genDateTimeSubFunc("minusMinutes", myDateTime.content, daysToSub.content)

  // Creates a DateTime object that is the result of adding the specified number of years
  // to the specified DateTime.
  // Example: #{plusYears(myDateTime,1)}
  def plusYears(myDateTime: DateTimeExp, yearsToAdd: NumericExp) =
    genDateTimeSubFunc("plusYears", myDateTime.content, yearsToAdd.content)

  // Creates a DateTime object that is the result of adding the specified number of months
  // to the specified DateTime.
  // Example: #{plusMonths(myDateTime,1)}
  def plusMonths(myDateTime: DateTimeExp, monthsToAdd: NumericExp) =
    genDateTimeSubFunc("plusMonths", myDateTime.content, monthsToAdd.content)

  // Creates a DateTime object that is the result of adding the specified number of weeks
  // to the specified DateTime.
  // Example: #{plusWeeks(myDateTime,1)}
  def plusWeeks(myDateTime: DateTimeExp, weeksToAdd: NumericExp) =
    genDateTimeSubFunc("plusWeeks", myDateTime.content, weeksToAdd.content)

  // Creates a DateTime object that is the result of adding the specified number of days
  // to the specified DateTime.
  // Example: #{plusDays(myDateTime,1)}
  def plusDays(myDateTime: DateTimeExp, daysToAdd: NumericExp) =
    genDateTimeSubFunc("plusDays", myDateTime.content, daysToAdd.content)

  // Creates a DateTime object that is the result of adding the specified number of hours
  // to the specified DateTime.
  // Example: #{plusHours(myDateTime,1)}
  def plusHours(myDateTime: DateTimeExp, hoursToAdd: NumericExp) =
    genDateTimeSubFunc("plusHours", myDateTime.content, hoursToAdd.content)

  // Creates a DateTime object that is the result of adding the specified number of minutes
  // to the specified DateTime.
  // Example: #{plusMinutes(myDateTime,1)}
  def plusMinutes(myDateTime: DateTimeExp, minutesToAdd: NumericExp) =
    genDateTimeSubFunc("plusMinutes", myDateTime.content, minutesToAdd.content)

  private def genDateTimeSubFunc(funcName: String, args: String*) =
    new DateTimeExp(s"$funcName(${args.mkString(",")})")

  private def genNumericSubFunc(funcName: String, args: String*) =
    new NumericExp(s"$funcName(${args.mkString(",")})")

}

object DateTimeFunctions extends DateTimeFunctions
