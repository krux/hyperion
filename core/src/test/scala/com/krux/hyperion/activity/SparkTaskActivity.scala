package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.adt.HString
import com.krux.hyperion.resource.SparkCluster
import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class SparkTaskActivitySpec extends FlatSpec {
  implicit val hc = new HyperionContext(ConfigFactory.load("example"))

  class MainClass

  it should "handle EMR release 4.x.x" in {
    val cluster = SparkCluster().withReleaseLabel("emr-4.6.0")
    val activity = SparkTaskActivity("something.jar", MainClass)(cluster)
    activity.scriptRunner shouldBe ("command-runner.jar": HString)
    activity.jobRunner shouldBe ("spark-submit": HString)
  }

  it should "be backwards compatible" in {
    val cluster = SparkCluster()
    val activity = SparkTaskActivity("something.jar", MainClass)(cluster)
    activity.scriptRunner shouldBe ("s3://elasticmapreduce/libs/script-runner/script-runner.jar": HString)
    assert(activity.jobRunner.toString.endsWith("run-spark-step.sh"))
  }
}
