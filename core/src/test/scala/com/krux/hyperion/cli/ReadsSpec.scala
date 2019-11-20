package com.krux.hyperion.cli

import java.time.{DayOfWeek, ZonedDateTime}
import org.scalatest.WordSpec

class ReadsSpec extends WordSpec {
  "`with` function of ZonedDateTime" should {
    "shift the date to the expected" in {

      val dt = ZonedDateTime.parse("2019-11-20T00:00:00Z")

      assert(
          dt.`with`(DayOfWeek.MONDAY)    === ZonedDateTime.parse("2019-11-18T00:00:00Z") &&
          dt.`with`(DayOfWeek.TUESDAY)   === ZonedDateTime.parse("2019-11-19T00:00:00Z") &&
          dt.`with`(DayOfWeek.WEDNESDAY) === ZonedDateTime.parse("2019-11-20T00:00:00Z") &&
          dt.`with`(DayOfWeek.THURSDAY)  === ZonedDateTime.parse("2019-11-21T00:00:00Z") &&
          dt.`with`(DayOfWeek.FRIDAY)    === ZonedDateTime.parse("2019-11-22T00:00:00Z") &&
          dt.`with`(DayOfWeek.SATURDAY)  === ZonedDateTime.parse("2019-11-23T00:00:00Z") &&
          dt.`with`(DayOfWeek.SUNDAY)    === ZonedDateTime.parse("2019-11-24T00:00:00Z")
      )
    }
  }
}
