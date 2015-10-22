package com.krux.hyperion.expression

import org.scalatest.WordSpec

class DateTimeFunctionsSpec extends WordSpec {

  "Format" should {
    "return the correct function String" in {
      val result = """#{format(@actualStartTime,'yyyy-MM-dd')}"""

      val expr = Format(RunnableObject.ActualStartTime, "yyyy-MM-dd")

      assert(expr.toString === result)
    }
  }

}
