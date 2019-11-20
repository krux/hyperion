package com.krux.hyperion.expression

import org.scalatest.WordSpec

class DateTimeExprSpec extends WordSpec {
  "+" should {
    "add hour period to DateTime expression" in {
      val expectation = PlusHours(RunnableObject.ActualStartTime,2)

      val periodDuration = PeriodDuration.hours(2)

      val expr = RunnableObject.ActualStartTime +  periodDuration

      assert(expr === expectation)
    }

    "add complex period to DateTime expression" in {
      val expectation = PlusMinutes(PlusHours(PlusDays(PlusWeeks(PlusMonths(PlusYears(RunnableObject.ActualStartTime,1),1),1),1),1),10)

      val periodDuration = PeriodDuration(year = 1, month = 1, week = 1, day = 1, hour = 1, minute = 10)

      val expr = RunnableObject.ActualStartTime + periodDuration

      assert(expr === expectation)
    }
  }
}
