package com.krux.hyperion.common

import org.joda.time.Period

import com.krux.hyperion.expression._


/**
 * Handles the duration conversion between expression and Joda time
 */
object DurationConverters {

  implicit class AsJodaPeriod(duration: Duration) {

    def asJodaPeriodMultiplied(multiplier: Int): Period = duration match {
      case Year(n) => Period.years(n * multiplier)
      case Month(n) => Period.months(n * multiplier)
      case Week(n) => Period.weeks(n * multiplier)
      case Day(n) => Period.days(n * multiplier)
      case Hour(n) => Period.hours(n * multiplier)
      case Minute(n) => Period.minutes(n * multiplier)
    }

  }

}
