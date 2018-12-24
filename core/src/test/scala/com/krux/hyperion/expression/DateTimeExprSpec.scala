package com.krux.hyperion.expression

import org.joda.time.Period
import org.scalatest.WordSpec

class DateTimeExprSpec extends WordSpec {

  "+" should {
    "add hour period to DateTime expression" in {
      val expectation = PlusHours(RunnableObject.ActualStartTime,2)

      val period = Period.hours(2)

      val expr = RunnableObject.ActualStartTime + period
      assert(expr === expectation)
    }

    "add complex period to DateTime expression" in {
      val expectation = PlusMinutes(PlusHours(PlusDays(PlusWeeks(PlusMonths(PlusYears(RunnableObject.ActualStartTime,1),1),1),1),1),10)

      val period = Period.years(1)
        .withMonths(1)
        .withWeeks(1)
        .withDays(1)
        .withHours(1)
        .withMinutes(10)

      val expr = RunnableObject.ActualStartTime + period
      assert(expr === expectation)
    }
  }
}
