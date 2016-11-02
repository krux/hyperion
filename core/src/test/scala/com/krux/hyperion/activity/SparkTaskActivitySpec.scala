package com.krux.hyperion.activity

import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import com.krux.hyperion.adt.HString
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.resource.SparkCluster

class SparkTaskActivitySpec extends FlatSpec {
  implicit val hc = new HyperionContext(ConfigFactory.load("example"))

  class MainClass

  it should "handle EMR release 4.x.x" in {
    val cluster = SparkCluster().withReleaseLabel("emr-4.6.0")
    val activity = SparkTaskActivity("something.jar", MainClass)(cluster)
    activity.scriptRunner.shouldBe("s3://elasticmapreduce/libs/script-runner/script-runner.jar": HString)
    activity.jobRunner.shouldBe("s3://your-bucket/datapipeline/scripts/run-spark-step-release-label.sh": HString)
  }

  it should "be backwards compatible" in {
    val cluster = SparkCluster()
    val activity = SparkTaskActivity("something.jar", MainClass)(cluster)
    activity.scriptRunner shouldBe ("s3://elasticmapreduce/libs/script-runner/script-runner.jar": HString)
    assert(activity.jobRunner.toString.endsWith("run-spark-step.sh"))
  }
}
