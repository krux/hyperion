package com.krux.hyperion.aws

import org.json4s.JsonDSL._
import org.scalatest.WordSpec

class AdpDataFormatsSpec extends WordSpec {
  "TsvDataFormat" should {
    "converts to Json" in {
      val testObj = AdpTsvDataFormat(
        "tsv",
        None,
        None,
        Some("\\")
      )
      val objShouldBe = ("id" -> "tsv") ~
        ("escapeChar" -> "\\") ~
        ("type" -> "TSV")

      assert(AdpJsonSerializer(testObj) === objShouldBe)
    }
  }
}
