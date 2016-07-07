package com.krux.hyperion.resource

import com.krux.hyperion.HyperionContext
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class SparkClusterSpec extends FlatSpec {

  implicit val hc = new HyperionContext()
  it should "handle release Label 4.x.x" in {
    val cluster = SparkCluster().withReleaseLabel("emr-4.6.0")
    cluster.standardBootstrapAction.length shouldBe 0
    cluster.applications.length shouldBe 1
  }

  it should "be backwards compatible" in {
    val cluster = SparkCluster()
    cluster.standardBootstrapAction.length shouldBe 1
    cluster.applications.length shouldBe 0
  }
}
