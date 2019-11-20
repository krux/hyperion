package com.krux.hyperion.adt

import java.time.{ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

import org.scalatest.WordSpec

class HTypeSpec extends WordSpec {
  "HDateTime" should {
    "be serialized in the correct datetime format" in {

      val dt: HDateTime = ZonedDateTime.parse("2014-04-02T00:00:00Z")

      assert(dt.serialize === "2014-04-02T00:00:00")
    }
  }

  "withZoneSameLocal" should {
    "change the time zone but retain the datetime" in {

      val datetimeFormat = DateTimeFormatter.ofPattern( "yyyy-MM-dd'T'HH:mm:ss")

      val dt = ZonedDateTime.parse("2019-11-18T00:00:00Z")

      val dtUtc = dt.withZoneSameLocal(ZoneId.of("UTC"))

      assert(
        dt.format(datetimeFormat) === dtUtc.format(datetimeFormat) &&
        dtUtc.getZone === ZoneId.of("UTC"))
    }
  }
}
