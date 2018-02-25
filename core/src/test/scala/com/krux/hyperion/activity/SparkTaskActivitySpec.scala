package com.krux.hyperion.activity

import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import com.krux.hyperion.adt.HString
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.resource.{EmrCluster, EmrApplication}
import com.krux.hyperion.common.S3Uri._


class SparkTaskActivitySpec extends FlatSpec {
  implicit val hc = new HyperionContext(ConfigFactory.load("example"))

  class MainClass

  it should "handle script runner" in {
    val cluster = EmrCluster().withApplications(EmrApplication.Spark).withReleaseLabel("emr-4.6.0")
    val activity = SparkTaskActivity(s3"something.jar")(cluster).withMainClass(MainClass)
    activity.jarUri.shouldBe("s3://elasticmapreduce/libs/script-runner/script-runner.jar": HString)
    activity.command.shouldBe("s3://your-bucket/datapipeline/scripts/run-spark-step-release-label.sh": HString)
  }

  it should "handle command runner" in {
    val cluster = EmrCluster().withApplications(EmrApplication.Spark)
    val activity = SparkTaskActivity.commandRunner("something.jar")(cluster).withMainClass(MainClass)
    activity.jarUri.shouldBe("command-runner.jar": HString)
    activity.command.shouldBe("spark-submit": HString)
  }
}
