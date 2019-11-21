package com.krux.hyperion

import java.time.format.DateTimeFormatter
import java.time.{DayOfWeek, ZoneId, ZonedDateTime}

import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.WordSpec

class ScheduleSpec extends WordSpec{
  "`with` function of DayOfWeek" should {
    "shift the date to the expected" in {

      val dt = ZonedDateTime.parse("2019-11-20T00:00:00Z")

      assert(
        dt.`with`(DayOfWeek.of(1)) === ZonedDateTime.parse("2019-11-18T00:00:00Z") &&
        dt.`with`(DayOfWeek.of(2)) === ZonedDateTime.parse("2019-11-19T00:00:00Z") &&
        dt.`with`(DayOfWeek.of(3)) === ZonedDateTime.parse("2019-11-20T00:00:00Z") &&
        dt.`with`(DayOfWeek.of(4)) === ZonedDateTime.parse("2019-11-21T00:00:00Z") &&
        dt.`with`(DayOfWeek.of(5)) === ZonedDateTime.parse("2019-11-22T00:00:00Z") &&
        dt.`with`(DayOfWeek.of(6)) === ZonedDateTime.parse("2019-11-23T00:00:00Z") &&
        dt.`with`(DayOfWeek.of(7)) === ZonedDateTime.parse("2019-11-24T00:00:00Z")
      )
    }
  }

  "`with` function of DayOfWeek" should {
    "shift the date same as withDayOfWeek in Joda time" in {

      val dateTimeFormatStr = "yyyy-MM-dd'T'HH:mm:ss"
      val datetimeFormat = DateTimeFormatter.ofPattern(dateTimeFormatStr)

      val javaDt = ZonedDateTime.parse("2019-11-20T00:00:00Z").withZoneSameInstant(ZoneId.of("UTC"))
      val jodaDt = new DateTime("2019-11-20T00:00:00Z").withZone(DateTimeZone.UTC)

      assert(
        javaDt.`with`(DayOfWeek.of(1)).format(datetimeFormat) === jodaDt.withDayOfWeek(1).toString(dateTimeFormatStr) &&
        javaDt.`with`(DayOfWeek.of(2)).format(datetimeFormat) === jodaDt.withDayOfWeek(2).toString(dateTimeFormatStr) &&
        javaDt.`with`(DayOfWeek.of(3)).format(datetimeFormat) === jodaDt.withDayOfWeek(3).toString(dateTimeFormatStr) &&
        javaDt.`with`(DayOfWeek.of(4)).format(datetimeFormat) === jodaDt.withDayOfWeek(4).toString(dateTimeFormatStr) &&
        javaDt.`with`(DayOfWeek.of(5)).format(datetimeFormat) === jodaDt.withDayOfWeek(5).toString(dateTimeFormatStr) &&
        javaDt.`with`(DayOfWeek.of(6)).format(datetimeFormat) === jodaDt.withDayOfWeek(6).toString(dateTimeFormatStr) &&
        javaDt.`with`(DayOfWeek.of(7)).format(datetimeFormat) === jodaDt.withDayOfWeek(7).toString(dateTimeFormatStr)
      )
    }
  }

  "withZoneSameLocal" should {
    "be consistent with toDateTime in joda time" in {

      val dateTimeFormatStr = "yyyy-MM-dd'T'HH:mm:ss"
      val datetimeFormat = DateTimeFormatter.ofPattern( dateTimeFormatStr)

      val javaDt = ZonedDateTime.parse("2019-11-18T00:00:00Z").withZoneSameLocal(ZoneId.of("UTC"))
      val jodaDt = new DateTime("2019-11-18T00:00:00Z").toDateTime(DateTimeZone.UTC)

      assert(javaDt.format(datetimeFormat) === jodaDt.toString(dateTimeFormatStr))
    }
  }
}
