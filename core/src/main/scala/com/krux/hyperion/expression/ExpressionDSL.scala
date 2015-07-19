package com.krux.hyperion.expression

import PeriodUnit._
import scala.language.implicitConversions

object ExpressionDSL {

  implicit def dateTimeExp2Dsl(dt: DateTimeExp): DateTimeExpDSL = new DateTimeExpDSL(dt)
  implicit def dateTimeRef2Dsl(dt: DateTimeRef.Value): DateTimeExpDSL =
    new DateTimeExpDSL(new DateTimeExp(dt.toString))

  class DateTimeExpDSL(dt: DateTimeExp) {
    def - (period: DpPeriod): DateTimeExp = {
      period match {
        case DpPeriod(n, Minute) => DateTimeFunctions.minusMinutes(dt, n)
        case DpPeriod(n, Hour) => DateTimeFunctions.minusHours(dt, n)
        case DpPeriod(n, Day) => DateTimeFunctions.minusDays(dt, n)
        case DpPeriod(n, Week) => DateTimeFunctions.minusWeeks(dt, n)
        case DpPeriod(n, Month) => DateTimeFunctions.minusMonths(dt, n)
        case DpPeriod(n, Year) => DateTimeFunctions.minusYears(dt, n)
      }
    }
  }

}
