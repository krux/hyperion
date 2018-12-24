package com.krux.hyperion.aws

import org.json4s.JsonDSL._
import org.scalatest.WordSpec

class AdpScheduleSpec extends WordSpec {

  "AdpStartAtSchedule" should {
    "converts to Json" in {
      val testObj = AdpRecurringSchedule(
        id = "theId",
        name = None,
        period = "1 day",
        startAt = Option("FIRST_ACTIVATION_DATE_TIME"),
        startDateTime = None,
        endDateTime = None,
        occurrences = Option("1")
      )
      val objShouldBe = ("id" -> "theId") ~
        ("period" -> "1 day") ~
        ("startAt" -> "FIRST_ACTIVATION_DATE_TIME") ~
        ("occurrences" -> "1") ~
        ("type" -> "Schedule")

      assert(AdpJsonSerializer.apply(testObj) === objShouldBe)
    }
  }

  "AdpStartDateTimeSchedule" should {
    "converts to Json" in {
      val testObj = AdpRecurringSchedule(
        id = "theId",
        name = Option("SomeName"),
        period = "1 day",
        startAt = None,
        startDateTime = Option("2014-04-02T00:00:00"),
        endDateTime = None,
        occurrences = None
      )
      val objShouldBe = ("id" -> "theId") ~
        ("name" -> "SomeName") ~
        ("period" -> "1 day") ~
        ("startDateTime" -> "2014-04-02T00:00:00") ~
        ("type" -> "Schedule")

      assert(AdpJsonSerializer.apply(testObj) === objShouldBe)
    }
  }

}
