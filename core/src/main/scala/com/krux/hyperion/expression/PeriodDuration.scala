package com.krux.hyperion.expression

import java.time.Period

/**
 * Bridges java.time.Period and java.time.Duration
 * to be consistent with org.joda.time.Period
 */
case class PeriodDuration
(
  year: Int,
  month: Int,
  week: Int,
  day: Int,
  hour: Int,
  minute: Int
)
{
  private final val DAYS_PER_WEEK = 7
  private def createPeriodAndDuration: (Period, java.time.Duration) =
    (
      Period.ofYears(year)
        .plusMonths(month)
        .plusDays(Math.multiplyExact(week, DAYS_PER_WEEK))
        .plusDays(day),

      java.time.Duration.ofHours(hour)
        .plusMinutes(minute)
    )

  def toDuration: java.time.Duration = {
    val (period, duration) = createPeriodAndDuration
    java.time.Duration.ofDays(period.getDays).plus(duration)
  }
}

object PeriodDuration {
  def years(years: Int): PeriodDuration     = PeriodDuration(years, 0,  0, 0, 0, 0)
  def months(months: Int): PeriodDuration   = PeriodDuration(0, months, 0, 0, 0, 0)
  def weeks(weeks: Int): PeriodDuration     = PeriodDuration(0, 0, weeks,  0, 0, 0)
  def days(days: Int): PeriodDuration       = PeriodDuration(0, 0, 0, days,   0, 0)
  def hours(hours: Int): PeriodDuration     = PeriodDuration(0, 0, 0, 0, hours,  0)
  def minutes(minutes: Int): PeriodDuration = PeriodDuration(0, 0, 0, 0, 0, minutes)
}
