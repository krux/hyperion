package com.krux.hyperion.resource

import com.krux.hyperion.adt.HType
import com.krux.hyperion.aws.AdpEmrConfiguration
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen._
import org.scalatest.Inside._
import org.scalatest.prop.PropertyChecks._
import org.scalatest.{FlatSpec, Matchers}

class EmrConfigurationSpec extends FlatSpec with Matchers {

  it should "not emit empty lists" in {
    implicit lazy val genEmrConfig = Arbitrary(EmrConfiguration())

    implicit lazy val genProperty = Arbitrary(for {key <- alphaStr; value <- alphaStr}
      yield Property(HType.string2HString(key), HType.string2HString(value)))

    forAll { (config: List[EmrConfiguration], props: List[Property], classification: Option[String]) =>
      val emrConfig = EmrConfiguration()
        .withConfiguration(config: _*)
        .withProperty(props: _*)

      val ser = classification
        .fold(emrConfig)(emrConfig.withClassification(_))
        .serialize

      inside(ser) { case AdpEmrConfiguration(_, _, _, prop, conf) =>
        conf shouldNot be(Some(Seq()))
        prop shouldNot be(Some(Seq()))
      }
    }
  }
}