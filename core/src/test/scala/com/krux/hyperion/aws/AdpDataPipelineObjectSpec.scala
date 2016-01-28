package com.krux.hyperion.aws

import org.json4s.JsonDSL._
import org.scalatest.WordSpec

class AdpDataPipelineObjectSpec extends WordSpec {

  class TestObject extends AdpDataPipelineObject {
    val id: String = "TestId"
    val name: Option[String] = None
    val `type`: String = "TestObject"
  }

  class TestObjectWithName extends AdpDataPipelineObject {
    val id: String = "TestId"
    val name: Option[String] = Some("TestName")
    val `type`: String = "TestObject"
  }

  "DataPipelineObject" should {

    "produce JSON" in {
      val resultShouldBe = ("id" -> "TestId") ~ ("type" -> "TestObject")
      val testObj = new TestObject()
      assert(AdpJsonSerializer(testObj) === resultShouldBe)
    }

  }
}
