package com.krux.hyperion.expression

import org.joda.time.DateTime
import org.scalatest.{ OptionValues, WordSpec }

import com.krux.hyperion.aws.AdpParameter

class ParameterSpec extends WordSpec with OptionValues {
  "Parameter[DateTime]" should {
    "be serialized in the correct datetime format" in {
      implicit val values = new ParameterValues()

      val dateTimeParam = Parameter[DateTime]("datetime", new DateTime("2014-04-02T00:00:00Z"))

      assert(dateTimeParam.serialize.value === AdpParameter(
        id = "my_datetime",
        `default` = Some("2014-04-02T00:00:00")
      ))
    }
  }
}
