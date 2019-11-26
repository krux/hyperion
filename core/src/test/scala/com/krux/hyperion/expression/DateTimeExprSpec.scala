package com.krux.hyperion.expression

import org.scalatest.WordSpec

class DateTimeExprSpec extends WordSpec {
  "+" should {
    "add hour period to DateTime expression" in {
      val expectation = PlusMinutes(RunnableObject.ActualStartTime,120)

      val duration = java.time.Duration.ofHours(2)

      val expr = RunnableObject.ActualStartTime + duration

      assert(expr === expectation)
    }

    "add complex period to DateTime expression" in {
      val expectation = PlusMinutes(RunnableObject.ActualStartTime,1510)

      val duration = java.time.Duration.ofDays(1).plusHours(1).plusMinutes(10)

      val expr = RunnableObject.ActualStartTime + duration

      assert(expr === expectation)
    }
  }
}
