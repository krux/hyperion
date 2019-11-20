package com.krux.hyperion.adt

import java.time.{ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.WordSpec

class HTypeSpec extends WordSpec {
  "HDateTime" should {
    "be serialized in the correct datetime format" in {

      val dt: HDateTime = ZonedDateTime.parse("2014-04-02T00:00:00Z")

      assert(dt.serialize === "2014-04-02T00:00:00")
    }
  }

  "withZoneSameInstant" should {
    "be consistent with withZone in joda time" in {

      val dateTimeFormatStr = "yyyy-MM-dd'T'HH:mm:ss"
      val datetimeFormat = DateTimeFormatter.ofPattern( dateTimeFormatStr)

      val javaDt = ZonedDateTime.parse("2019-11-18T00:00:00Z").withZoneSameInstant(ZoneId.of("UTC"))

      val jodaDt = new DateTime("2019-11-18T00:00:00Z").withZone(DateTimeZone.UTC)

      assert(javaDt.format(datetimeFormat) === jodaDt.toString(dateTimeFormatStr))
    }
  }
}
