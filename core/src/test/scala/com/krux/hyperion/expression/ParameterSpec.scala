package com.krux.hyperion.expression

import java.time.ZonedDateTime
import org.scalatest.{ OptionValues, WordSpec }

import com.krux.hyperion.aws.AdpParameter

class ParameterSpec extends WordSpec with OptionValues {
  "Parameter[ZonedDateTime]" should {
    "be serialized in the correct datetime format" in {
      implicit val values = new ParameterValues()

      val dateTimeParam = Parameter[ZonedDateTime]("datetime", ZonedDateTime.parse("2014-04-02T00:00:00Z"))

      assert(dateTimeParam.serialize.value === AdpParameter(
        id = "my_datetime",
        `default` = Some("2014-04-02T00:00:00")
      ))
    }
  }
}
