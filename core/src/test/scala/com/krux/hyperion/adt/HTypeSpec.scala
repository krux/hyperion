package com.krux.hyperion.adt
import org.joda.time.DateTime
import org.scalatest.WordSpec

class HTypeSpec extends WordSpec {
  "HDateTime" should {
    "be serialized in the correct datetime format" in {

      val dt: HDateTime = new DateTime("2014-04-02T00:00:00Z")

      assert(dt.serialize === "2014-04-02T00:00:00")
    }
  }
}
