package com.krux.hyperion.common

import java.time.Period
import com.krux.hyperion.expression._

/**
 * Handles the duration conversion between expression and java.time.Duration
 */
object DurationConverters {

  implicit class AsJavaDuration(duration: Duration) {

    def asDurationMultiplied(multiplier: Int): java.time.Duration = duration match {
      case Year(n)   => java.time.Duration.ofDays(Period.ofYears(n * multiplier).getDays)
      case Month(n)  => java.time.Duration.ofDays(Period.ofMonths(n * multiplier).getDays)
      case Week(n)   => java.time.Duration.ofDays(Period.ofWeeks(n * multiplier).getDays)
      case Day(n)    => java.time.Duration.ofDays(n * multiplier)
      case Hour(n)   => java.time.Duration.ofHours(n * multiplier)
      case Minute(n) => java.time.Duration.ofMinutes(n * multiplier)
    }

  }
}
