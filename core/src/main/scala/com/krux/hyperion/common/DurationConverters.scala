package com.krux.hyperion.common

import com.krux.hyperion.expression._

/**
 * Handles the duration conversion between expression and PeriodDuration
 */
object DurationConverters {

  implicit class AsPeriodDuration(duration: Duration) {

    def asPeriodDurationMultiplied(multiplier: Int): PeriodDuration = duration match {
      case Year(n) => PeriodDuration.years(n * multiplier)
      case Month(n) => PeriodDuration.months(n * multiplier)
      case Week(n) => PeriodDuration.weeks(n * multiplier)
      case Day(n) => PeriodDuration.days(n * multiplier)
      case Hour(n) => PeriodDuration.hours(n * multiplier)
      case Minute(n) => PeriodDuration.minutes(n * multiplier)
    }

  }
}
