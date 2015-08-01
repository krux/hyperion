package com.krux.hyperion.expression

import scala.language.implicitConversions

object ExpressionDSL {

  implicit def dateTimeExp2Dsl(dt: DateTimeExp): DateTimeExpDSL = new DateTimeExpDSL(dt)

  implicit def dateTimeRef2Dsl(dt: DateTimeRef.Value): DateTimeExpDSL =
    new DateTimeExpDSL(new DateTimeExp(dt.toString))

  class DateTimeExpDSL(dt: DateTimeExp) {
    def - (period: DpPeriod): DateTimeExp = {
      period match {
        case Minute(n) => DateTimeFunctions.minusMinutes(dt, n)
        case Hour(n) => DateTimeFunctions.minusHours(dt, n)
        case Day(n) => DateTimeFunctions.minusDays(dt, n)
        case Week(n) => DateTimeFunctions.minusWeeks(dt, n)
        case Month(n) => DateTimeFunctions.minusMonths(dt, n)
        case Year(n) => DateTimeFunctions.minusYears(dt, n)
      }
    }
  }

}
