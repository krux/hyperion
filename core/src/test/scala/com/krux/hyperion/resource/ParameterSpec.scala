package com.krux.hyperion.resource

import com.krux.hyperion.expression.{Parameter, ParameterValues}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class ParameterSpec extends FlatSpec {
  implicit val pv = new ParameterValues

  it should "handle comparing int params without values" in {
    noException should be thrownBy Parameter[Int]("Param").compare(0)
  }

  it should "handle comparing double params without values" in {
    noException should be thrownBy Parameter[Double]("Param").compare(0)
  }

  it should "handle comparisons with values" in {
    (Parameter[Int]("Param").withValue(1) > 0) === Some(true)
    (Parameter[Int]("Param").withValue(1) < 0) === Some(false)
    (Parameter[Int]("Param").encrypted.withValue(1) > 0) === Some(true)
    (Parameter[Int]("Param").encrypted.withValue(1) < 0) === Some(false)
  }
}
