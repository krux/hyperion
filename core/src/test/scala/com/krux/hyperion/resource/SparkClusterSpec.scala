package com.krux.hyperion.resource

import com.krux.hyperion.HyperionContext
import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

@deprecated("Use EmrCluster instead", "5.0.0")
class SparkClusterSpec extends FlatSpec {
  implicit val hc = new HyperionContext(ConfigFactory.load("example"))

  it should "be handle old spark versions" in {
    val cluster = SparkCluster()
    cluster.standardBootstrapAction.length shouldBe 2
    cluster.applications.length shouldBe 0
  }
}
